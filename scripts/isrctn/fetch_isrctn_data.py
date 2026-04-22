import sys
import requests
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime
from scripts.config import ISRCTN_DB_CONFIG

# Namespaces used in ISRCTN XML
NS = {
    'isr': 'http://www.67bricks.com/isrctn',
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**ISRCTN_DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def parse_date(date_str):
    if not date_str:
        return None
    try:
        # 2026-04-20T09:21:05.295891682Z or 2024-12-03T00:00:00.000Z
        return date_str.split('T')[0]
    except:
        return None

def fetch_and_store_trial(isrctn_id, conn):
    url = f"https://www.isrctn.com/api/trial/{isrctn_id}/format/default"
    print(f"Fetching {isrctn_id}...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        xml_data = response.text
        root = ET.fromstring(xml_data)
    except Exception as e:
        print(f"Failed to fetch or parse {isrctn_id}: {e}")
        return False, str(e)

    cur = conn.cursor()
    try:
        # 1. Trial Metadata & Titles
        trial_elem = root.find('isr:trial', NS)
        trial_desc = trial_elem.find('isr:trialDescription', NS)
        
        cur.execute("""
            INSERT INTO trials (
                isrctn_id, last_updated_xml, version_xml, is_visible_to_public,
                public_id_type, public_id_canonical, public_id_date, isrctn_date_assigned,
                acknowledgment, title, scientific_title, acronym, study_hypothesis, plain_english_summary,
                study_design, primary_study_design, secondary_study_design, trial_types, overall_end_date,
                inclusion_criteria, exclusion_criteria, ethics_approval_required,
                rect_start_status_override, rect_status_override,
                ipd_sharing_plan, ipd_sharing_statement, data_policy, raw_xml
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (isrctn_id) DO UPDATE SET
                last_updated_xml = EXCLUDED.last_updated_xml,
                version_xml = EXCLUDED.version_xml,
                raw_xml = EXCLUDED.raw_xml,
                updated_at = CURRENT_TIMESTAMP
        """, (
            isrctn_id,
            trial_elem.get('lastUpdated'),
            trial_elem.get('version'),
            trial_elem.get('isVisibleToPublic') == 'true',
            trial_elem.get('publicIdentifierType'),
            trial_elem.get('publicIdentifierCanonical'),
            parse_date(trial_elem.get('publicIdentifierDateAssigned')),
            parse_date(root.find('isr:trial/isr:isrctn', NS).get('dateAssigned')) if root.find('isr:trial/isr:isrctn', NS) is not None else None,
            trial_desc.get('thirdPartyFilesAcknowledgement') == 'true',
            trial_desc.findtext('isr:title', namespaces=NS),
            trial_desc.findtext('isr:scientificTitle', namespaces=NS),
            trial_desc.findtext('isr:acronym', namespaces=NS),
            trial_desc.findtext('isr:studyHypothesis', namespaces=NS),
            trial_desc.findtext('isr:plainEnglishSummary', namespaces=NS),
            trial_elem.find('isr:trialDesign/isr:studyDesign', NS).text if trial_elem.find('isr:trialDesign/isr:studyDesign', NS) is not None else None,
            trial_elem.findtext('isr:trialDesign/isr:primaryStudyDesign', namespaces=NS),
            trial_elem.findtext('isr:trialDesign/isr:secondaryStudyDesign', namespaces=NS),
            trial_elem.findtext('isr:trialDesign/isr:trialTypes', namespaces=NS),
            parse_date(trial_elem.findtext('isr:trialDesign/isr:overallEndDate', namespaces=NS)),
            trial_elem.findtext('isr:participants/isr:inclusion', namespaces=NS),
            trial_elem.findtext('isr:participants/isr:exclusion', namespaces=NS),
            trial_desc.findtext('isr:ethicsApprovalRequired', namespaces=NS),
            trial_elem.findtext('isr:participants/isr:recruitmentStartStatusOverride', namespaces=NS),
            trial_elem.findtext('isr:participants/isr:recruitmentStatusOverride', namespaces=NS),
            root.find('isr:trial/isr:miscellaneous/isr:ipdSharingPlan', NS).text if root.find('isr:trial/isr:miscellaneous/isr:ipdSharingPlan', NS) is not None else None,
            root.findtext('isr:trial/isr:results/isr:ipdSharingStatement', namespaces=NS),
            root.findtext('isr:trial/isr:results/isr:dataPolicies/isr:dataPolicy', namespaces=NS),
            xml_data
        ))

        # 2. Participant details
        parts = trial_elem.find('isr:participants', NS)
        if parts is not None:
            lower_age = parts.find('isr:lowerAgeLimit', NS)
            upper_age = parts.find('isr:upperAgeLimit', NS)
            cur.execute("""
                INSERT INTO participant_details (
                    isrctn_id, age_range, lower_age_limit_value, lower_age_limit_unit,
                    upper_age_limit_value, upper_age_limit_unit, gender,
                    healthy_volunteers_allowed, target_enrolment, total_final_enrolment,
                    recruitment_start, recruitment_end
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isrctn_id) DO UPDATE SET
                    age_range = EXCLUDED.age_range, gender = EXCLUDED.gender
            """, (
                isrctn_id,
                parts.findtext('isr:ageRange', namespaces=NS),
                lower_age.get('value') if lower_age is not None else None,
                lower_age.get('unit') if lower_age is not None else None,
                upper_age.get('value') if upper_age is not None else None,
                upper_age.get('unit') if upper_age is not None else None,
                parts.findtext('isr:gender', namespaces=NS),
                parts.findtext('isr:healthyVolunteersAllowed', namespaces=NS) == 'true',
                parts.findtext('isr:targetEnrolment', namespaces=NS),
                parts.findtext('isr:totalFinalEnrolment', namespaces=NS),
                parse_date(parts.findtext('isr:recruitmentStart', namespaces=NS)),
                parse_date(parts.findtext('isr:recruitmentEnd', namespaces=NS))
            ))

        # 3. Outcomes
        cur.execute("DELETE FROM outcomes WHERE isrctn_id = %s", (isrctn_id,))
        for outcome_group in ['isr:primaryOutcomes', 'isr:secondaryOutcomes']:
            group_type = 'primary' if 'primary' in outcome_group else 'secondary'
            measures = trial_elem.find(outcome_group, NS)
            if measures is not None:
                for m in measures.findall('isr:outcomeMeasure', NS):
                    cur.execute("""
                        INSERT INTO outcomes (isrctn_id, measure_id, outcome_type, variable, method, timepoints)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        isrctn_id, m.get('id'), group_type,
                        m.findtext('isr:variable', namespaces=NS),
                        m.findtext('isr:method', namespaces=NS),
                        m.findtext('isr:timepoints', namespaces=NS)
                    ))

        # 4. Ethics Committees
        cur.execute("DELETE FROM ethics_committees WHERE isrctn_id = %s", (isrctn_id,))
        ethics = trial_desc.find('isr:ethicsCommittees', NS)
        if ethics is not None:
            for e in ethics.findall('isr:ethicsCommittee', NS):
                cd = e.find('isr:contactDetails', NS)
                cur.execute("""
                    INSERT INTO ethics_committees (
                        isrctn_id, committee_id, approval_status, status_date, committee_name,
                        address, city, state, country, zip, telephone, email, committee_reference
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    isrctn_id, e.get('id'), e.get('approvalStatus'), parse_date(e.get('statusDate')),
                    e.findtext('isr:committeeName', namespaces=NS),
                    cd.findtext('isr:address', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:city', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:state', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:country', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:zip', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:telephone', namespaces=NS) if cd is not None else None,
                    cd.findtext('isr:email', namespaces=NS) if cd is not None else None,
                    e.findtext('isr:committeeReference', namespaces=NS)
                ))

        # 5. External Identifiers
        refs = trial_elem.find('isr:externalRefs', NS)
        if refs is not None:
            cur.execute("""
                INSERT INTO external_identifiers (
                    isrctn_id, doi, eudract_number, iras_number, clinicaltrials_gov_number,
                    protocol_serial_number, secondary_numbers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isrctn_id) DO UPDATE SET
                    doi = EXCLUDED.doi, clinicaltrials_gov_number = EXCLUDED.clinicaltrials_gov_number
            """, (
                isrctn_id,
                refs.findtext('isr:doi', namespaces=NS),
                refs.findtext('isr:eudraCTNumber', namespaces=NS),
                refs.findtext('isr:irasNumber', namespaces=NS),
                refs.findtext('isr:clinicalTrialsGovNumber', namespaces=NS),
                refs.findtext('isr:protocolSerialNumber', namespaces=NS),
                refs.findtext('isr:secondaryNumbers', namespaces=NS)
            ))

        # 6. Centres & Countries
        cur.execute("DELETE FROM recruitment_countries WHERE isrctn_id = %s", (isrctn_id,))
        countries = parts.find('isr:recruitmentCountries', NS)
        if countries is not None:
            for c in countries.findall('isr:country', NS):
                cur.execute("INSERT INTO recruitment_countries (isrctn_id, country) VALUES (%s, %s)", (isrctn_id, c.text))

        cur.execute("DELETE FROM trial_centres WHERE isrctn_id = %s", (isrctn_id,))
        centres = parts.find('isr:trialCentres', NS)
        if centres is not None:
            for tc in centres.findall('isr:trialCentre', NS):
                cur.execute("""
                    INSERT INTO trial_centres (isrctn_id, centre_id, name, address, city, state, country, zip)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    isrctn_id, tc.get('id'),
                    tc.findtext('isr:name', namespaces=NS),
                    tc.findtext('isr:address', namespaces=NS),
                    tc.findtext('isr:city', namespaces=NS),
                    tc.findtext('isr:state', namespaces=NS),
                    tc.findtext('isr:country', namespaces=NS),
                    tc.findtext('isr:zip', namespaces=NS)
                ))

        # 7. Medical Info
        cur.execute("DELETE FROM conditions WHERE isrctn_id = %s", (isrctn_id,))
        conds = trial_elem.find('isr:conditions', NS)
        if conds is not None:
            for c in conds.findall('isr:condition', NS):
                cur.execute("INSERT INTO conditions (isrctn_id, description, disease_class1, disease_class2) VALUES (%s, %s, %s, %s)",
                    (isrctn_id, c.findtext('isr:description', namespaces=NS), c.findtext('isr:diseaseClass1', namespaces=NS), c.findtext('isr:diseaseClass2', namespaces=NS)))

        cur.execute("DELETE FROM interventions WHERE isrctn_id = %s", (isrctn_id,))
        inters = trial_elem.find('isr:interventions', NS)
        if inters is not None:
            for i in inters.findall('isr:intervention', NS):
                cur.execute("INSERT INTO interventions (isrctn_id, description, intervention_type, phase, drug_names) VALUES (%s, %s, %s, %s, %s)",
                    (isrctn_id, i.findtext('isr:description', namespaces=NS), i.findtext('isr:interventionType', namespaces=NS), i.findtext('isr:phase', namespaces=NS), i.findtext('isr:drugNames', namespaces=NS)))

        # 8. Organizations
        cur.execute("DELETE FROM organizations WHERE isrctn_id = %s", (isrctn_id,))
        for org in root.findall('isr:sponsor', NS) + root.findall('isr:funder', NS):
            role = 'SPONSOR' if 'sponsor' in org.tag else 'FUNDER'
            cur.execute("""
                INSERT INTO organizations (isrctn_id, org_id, name, org_role, org_type, ror_id, commercial_status, fund_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                isrctn_id, org.get('id'),
                org.findtext('isr:organisation', namespaces=NS) or org.findtext('isr:name', namespaces=NS),
                role, org.findtext('isr:sponsorType', namespaces=NS),
                org.findtext('isr:rorId', namespaces=NS),
                org.findtext('isr:commercialStatus', namespaces=NS),
                org.findtext('isr:fundRef', namespaces=NS)
            ))

        # 9. Contacts
        cur.execute("DELETE FROM contacts WHERE isrctn_id = %s", (isrctn_id,))
        for c in root.findall('isr:contact', NS):
            cd = c.find('isr:contactDetails', NS)
            cur.execute("""
                INSERT INTO contacts (
                    isrctn_id, contact_id, title, forename, surname, orcid,
                    address, city, state, country, zip, telephone, email, privacy
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                isrctn_id, c.get('id'), c.findtext('isr:title', namespaces=NS),
                c.findtext('isr:forename', namespaces=NS), c.findtext('isr:surname', namespaces=NS),
                c.findtext('isr:orcid', namespaces=NS),
                cd.findtext('isr:address', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:city', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:state', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:country', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:zip', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:telephone', namespaces=NS) if cd is not None else None,
                cd.findtext('isr:email', namespaces=NS) if cd is not None else None,
                c.findtext('isr:privacy', namespaces=NS)
            ))
            contact_record_id = cur.fetchone()[0]
            
            ctypes = c.find('isr:contactTypes', NS)
            if ctypes is not None:
                for ct in ctypes.findall('isr:contactType', NS):
                    cur.execute("INSERT INTO contact_types (contact_record_id, type_name) VALUES (%s, %s)", (contact_record_id, ct.text))

        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        print(f"Error processing data for {isrctn_id}: {e}")
        return False, str(e)
    finally:
        cur.close()

def main():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get pending trials
        cur.execute("SELECT isrctn_id FROM trial_queue WHERE retrieval_status = 'pending'")
        trials_to_fetch = [row[0] for row in cur.fetchall()]
        
        if not trials_to_fetch:
            print("No pending trials in queue.")
            return

        print(f"Starting retrieval of {len(trials_to_fetch)} trials...")
        
        for isrctn_id in trials_to_fetch:
            success, error_msg = fetch_and_store_trial(isrctn_id, conn)
            
            # Update queue status
            status = 'completed' if success else 'failed'
            cur.execute("""
                UPDATE trial_queue 
                SET retrieval_status = %s, 
                    last_attempt = CURRENT_TIMESTAMP,
                    error_log = %s
                WHERE isrctn_id = %s
            """, (status, error_msg, isrctn_id))
            conn.commit()
            
        print("Batch processing complete.")
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
