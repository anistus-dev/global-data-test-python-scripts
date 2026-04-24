import sys
import requests
import xml.etree.ElementTree as ET
import psycopg2
import traceback
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

def safe_int(val):
    if val is None:
        return None
    try:
        s = str(val).strip()
        if not s:
            return None
        return int(float(s)) # Handle case where it might be '80.0'
    except:
        return None

def safe_float(val):
    if val is None:
        return None
    try:
        s = str(val).strip()
        if not s:
            return None
        return float(s)
    except:
        return None

def fetch_and_store_trial(isrctn_id, conn):
    url = f"https://www.isrctn.com/api/trial/{isrctn_id}/format/default"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        xml_data = response.text
        root = ET.fromstring(xml_data)
    except Exception as e:
        # Return simplified error message for logging
        return False, str(e)

    cur = conn.cursor()
    try:
        # 1. Trial Elements
        trial_elem = root.find('isr:trial', NS)
        trial_desc = trial_elem.find('isr:trialDescription', NS)
        trial_design = trial_elem.find('isr:trialDesign', NS)
        results_elem = trial_elem.find('isr:results', NS)
        participants = trial_elem.find('isr:participants', NS)
        
        # Trial types can be a list or a tag
        trial_types_list = []
        if trial_design is not None:
            tt_elem = trial_design.find('isr:trialTypes', NS)
            if tt_elem is not None:
                trial_types_list = [t.text for t in tt_elem.findall('isr:trialType', NS) if t.text]
            if not trial_types_list and trial_design.findtext('isr:trialTypes', namespaces=NS):
                trial_types_list = [trial_design.findtext('isr:trialTypes', namespaces=NS)]
        trial_types_str = ", ".join(trial_types_list) if trial_types_list else None

        cur.execute("""
            INSERT INTO trials (
                isrctn_id, last_updated_xml, version_xml, is_visible_to_public,
                public_id_type, public_id_canonical, public_id_date, isrctn_date_assigned,
                acknowledgment, title, scientific_title, acronym, study_hypothesis, plain_english_summary,
                study_design, primary_study_design, secondary_study_design, trial_types, overall_end_date,
                overall_status_override, reason_abandoned,
                inclusion_criteria, exclusion_criteria, ethics_approval_required, ethics_approval_text,
                rect_start_status_override, rect_status_override,
                primary_outcome_text, secondary_outcome_text,
                publication_details, publication_stage, basic_report, plain_english_report,
                ipd_sharing_plan, ipd_sharing_statement, data_policy, raw_xml
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (isrctn_id) DO UPDATE SET
                last_updated_xml = EXCLUDED.last_updated_xml,
                version_xml = EXCLUDED.version_xml,
                raw_xml = EXCLUDED.raw_xml,
                updated_at = CURRENT_TIMESTAMP
        """, (
            isrctn_id,
            trial_elem.get('lastUpdated'),
            safe_int(trial_elem.get('version')),
            trial_elem.get('isVisibleToPublic') == 'true',
            trial_elem.get('publicIdentifierType'),
            trial_elem.get('publicIdentifierCanonical'),
            parse_date(trial_elem.get('publicIdentifierDateAssigned')),
            parse_date(trial_elem.find('isr:isrctn', NS).get('dateAssigned')) if trial_elem.find('isr:isrctn', NS) is not None else None,
            trial_desc.get('thirdPartyFilesAcknowledgement') == 'true',
            trial_desc.findtext('isr:title', namespaces=NS),
            trial_desc.findtext('isr:scientificTitle', namespaces=NS),
            trial_desc.findtext('isr:acronym', namespaces=NS),
            trial_desc.findtext('isr:studyHypothesis', namespaces=NS),
            trial_desc.findtext('isr:plainEnglishSummary', namespaces=NS),
            trial_design.findtext('isr:studyDesign', namespaces=NS) if trial_design is not None else None,
            trial_design.findtext('isr:primaryStudyDesign', namespaces=NS) if trial_design is not None else None,
            trial_design.findtext('isr:secondaryStudyDesign', namespaces=NS) if trial_design is not None else None,
            trial_types_str,
            parse_date(trial_design.findtext('isr:overallEndDate', namespaces=NS)) if trial_design is not None else None,
            trial_design.findtext('isr:overallStatusOverride', namespaces=NS) if trial_design is not None else None,
            trial_design.findtext('isr:reasonAbandoned', namespaces=NS) if trial_design is not None else None,
            participants.findtext('isr:inclusion', namespaces=NS) if participants is not None else None,
            participants.findtext('isr:exclusion', namespaces=NS) if participants is not None else None,
            trial_desc.findtext('isr:ethicsApprovalRequired', namespaces=NS),
            trial_desc.findtext('isr:ethicsApproval', namespaces=NS),
            participants.findtext('isr:recruitmentStartStatusOverride', namespaces=NS) if participants is not None else None,
            participants.findtext('isr:recruitmentStatusOverride', namespaces=NS) if participants is not None else None,
            trial_desc.findtext('isr:primaryOutcome', namespaces=NS),
            trial_desc.findtext('isr:secondaryOutcome', namespaces=NS),
            results_elem.findtext('isr:publicationDetails', namespaces=NS) if results_elem is not None else None,
            results_elem.findtext('isr:publicationStage', namespaces=NS) if results_elem is not None else None,
            results_elem.findtext('isr:basicReport', namespaces=NS) if results_elem is not None else None,
            results_elem.findtext('isr:plainEnglishReport', namespaces=NS) if results_elem is not None else None,
            trial_elem.findtext('isr:miscellaneous/isr:ipdSharingPlan', namespaces=NS),
            results_elem.findtext('isr:ipdSharingStatement', namespaces=NS) if results_elem is not None else None,
            results_elem.findtext('isr:dataPolicies/isr:dataPolicy', namespaces=NS) if results_elem is not None else None,
            xml_data
        ))

        # 2. Participant details
        if participants is not None:
            lower_age = participants.find('isr:lowerAgeLimit', NS)
            upper_age = participants.find('isr:upperAgeLimit', NS)
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
                participants.findtext('isr:ageRange', namespaces=NS),
                safe_float(lower_age.get('value')) if lower_age is not None else None,
                lower_age.get('unit') if lower_age is not None else None,
                safe_float(upper_age.get('value')) if upper_age is not None else None,
                upper_age.get('unit') if upper_age is not None else None,
                participants.findtext('isr:gender', namespaces=NS),
                participants.findtext('isr:healthyVolunteersAllowed', namespaces=NS) == 'true',
                safe_int(participants.findtext('isr:targetEnrolment', namespaces=NS)),
                safe_int(participants.findtext('isr:totalFinalEnrolment', namespaces=NS)),
                parse_date(participants.findtext('isr:recruitmentStart', namespaces=NS)),
                parse_date(participants.findtext('isr:recruitmentEnd', namespaces=NS))
            ))
            
            # Participant Types List
            cur.execute("DELETE FROM participant_types WHERE isrctn_id = %s", (isrctn_id,))
            pt_container = participants.find('isr:participantTypes', NS)
            if pt_container is not None:
                for pt in pt_container.findall('isr:participantType', NS):
                    cur.execute("INSERT INTO participant_types (isrctn_id, participant_type) VALUES (%s, %s)", (isrctn_id, pt.text))

        # 3. Outcomes (Structured)
        cur.execute("DELETE FROM outcomes WHERE isrctn_id = %s", (isrctn_id,))
        for outcome_group in ['isr:primaryOutcomes', 'isr:secondaryOutcomes']:
            group_type = 'primary' if 'primary' in outcome_group else 'secondary'
            measures_container = trial_desc.find(outcome_group, NS)
            if measures_container is not None:
                for m in measures_container.findall('isr:outcomeMeasure', NS):
                    cur.execute("""
                        INSERT INTO outcomes (isrctn_id, measure_id, outcome_type, variable, method, timepoints)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        isrctn_id, m.get('id'), group_type,
                        m.findtext('isr:variable', namespaces=NS),
                        m.findtext('isr:method', namespaces=NS),
                        m.findtext('isr:timepoints', namespaces=NS)
                    ))

        # 4. Trial Design (Interventional Details)
        cur.execute("DELETE FROM interventional_designs WHERE isrctn_id = %s", (isrctn_id,))
        cur.execute("DELETE FROM trial_purposes WHERE isrctn_id = %s", (isrctn_id,))
        itd = trial_design.find('isr:interventionalTrialDesign', NS) if trial_design is not None else None
        if itd is not None:
            cur.execute("""
                INSERT INTO interventional_designs (isrctn_id, allocation, masking, control, assignment)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                isrctn_id,
                itd.findtext('isr:allocation', namespaces=NS),
                itd.findtext('isr:masking', namespaces=NS),
                itd.findtext('isr:control', namespaces=NS),
                itd.findtext('isr:assignment', namespaces=NS)
            ))
            purposes = itd.find('isr:purposes', NS)
            if purposes is not None:
                for p in purposes.findall('isr:purpose', NS):
                    cur.execute("INSERT INTO trial_purposes (isrctn_id, purpose) VALUES (%s, %s)", (isrctn_id, p.text))

        # 5. External Identifiers & Secondary Numbers
        refs = trial_elem.find('isr:externalRefs', NS)
        if refs is not None:
            cur.execute("""
                INSERT INTO external_identifiers (
                    isrctn_id, doi, eudract_number, iras_number, clinicaltrials_gov_number, protocol_serial_number
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (isrctn_id) DO UPDATE SET
                    doi = EXCLUDED.doi, clinicaltrials_gov_number = EXCLUDED.clinicaltrials_gov_number
            """, (
                isrctn_id,
                refs.findtext('isr:doi', namespaces=NS),
                refs.findtext('isr:eudraCTNumber', namespaces=NS),
                refs.findtext('isr:irasNumber', namespaces=NS),
                refs.findtext('isr:clinicalTrialsGovNumber', namespaces=NS),
                refs.findtext('isr:protocolSerialNumber', namespaces=NS)
            ))
            
            cur.execute("DELETE FROM secondary_identifiers WHERE isrctn_id = %s", (isrctn_id,))
            sec_nums = refs.find('isr:secondaryNumbers', NS)
            if sec_nums is not None:
                for sn in sec_nums.findall('isr:secondaryNumber', NS):
                    cur.execute("""
                        INSERT INTO secondary_identifiers (isrctn_id, internal_id, number_type, canonical_number, value)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (isrctn_id, sn.get('id'), sn.get('numberType'), sn.get('canonicalSecondaryNumber'), sn.text))

        # 6. Centres & Countries
        if participants is not None:
            cur.execute("DELETE FROM recruitment_countries WHERE isrctn_id = %s", (isrctn_id,))
            countries = participants.find('isr:recruitmentCountries', NS)
            if countries is not None:
                for c in countries.findall('isr:country', NS):
                    cur.execute("INSERT INTO recruitment_countries (isrctn_id, country) VALUES (%s, %s)", (isrctn_id, c.text))

            cur.execute("DELETE FROM trial_centres WHERE isrctn_id = %s", (isrctn_id,))
            centres = participants.find('isr:trialCentres', NS)
            if centres is not None:
                for tc in centres.findall('isr:trialCentre', NS):
                    cur.execute("""
                        INSERT INTO trial_centres (isrctn_id, centre_id, rts_id, name, address, city, state, country, zip)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        isrctn_id, tc.get('id'), tc.findtext('isr:rtsId', namespaces=NS),
                        tc.findtext('isr:name', namespaces=NS),
                        tc.findtext('isr:address', namespaces=NS),
                        tc.findtext('isr:city', namespaces=NS),
                        tc.findtext('isr:state', namespaces=NS),
                        tc.findtext('isr:country', namespaces=NS),
                        tc.findtext('isr:zip', namespaces=NS)
                    ))

        # 7. Ethics Committees
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

        # 8. Medical Info
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
                dnames_list = [d.text for d in i.findall('isr:drugNames/isr:drugName', NS) if d.text]
                dnames_str = ", ".join(dnames_list) if dnames_list else i.findtext('isr:drugNames', namespaces=NS)
                cur.execute("INSERT INTO interventions (isrctn_id, description, intervention_type, phase, drug_names) VALUES (%s, %s, %s, %s, %s)",
                    (isrctn_id, i.findtext('isr:description', namespaces=NS), i.findtext('isr:interventionType', namespaces=NS), i.findtext('isr:phase', namespaces=NS), dnames_str))

        # 9. Outputs & Attached Files
        cur.execute("DELETE FROM data_outputs WHERE isrctn_id = %s", (isrctn_id,))
        outputs = trial_elem.find('isr:outputs', NS)
        if outputs is not None:
            for o in outputs.findall('isr:output', NS):
                lf = o.find('isr:localFile', NS)
                ext = o.find('isr:externalLink', NS)
                cur.execute("""
                    INSERT INTO data_outputs (
                        isrctn_id, output_xml_id, output_type, artefact_type, date_created, date_uploaded,
                        peer_reviewed, patient_facing, created_by, file_id, file_version, original_filename,
                        download_filename, mime_type, file_length, md5sum, description, production_notes, external_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    isrctn_id, o.get('id'), o.get('outputType'), o.get('artefactType'),
                    parse_date(o.get('dateCreated')), parse_date(o.get('dateUploaded')),
                    o.get('peerReviewed') == 'true', o.get('patientFacing') == 'true', o.get('createdBy'),
                    lf.get('fileId') if lf is not None else None,
                    lf.get('version') if lf is not None else None,
                    lf.get('originalFilename') if lf is not None else None,
                    lf.get('downloadFilename') if lf is not None else None,
                    lf.get('mimeType') if lf is not None else None,
                    safe_int(lf.get('length')) if lf is not None else None,
                    lf.get('md5sum') if lf is not None else None,
                    o.findtext('isr:description', namespaces=NS),
                    o.findtext('isr:productionNotes', namespaces=NS),
                    ext.get('url') if ext is not None else None
                ))

        cur.execute("DELETE FROM attached_files WHERE isrctn_id = %s", (isrctn_id,))
        attached = trial_elem.find('isr:attachedFiles', NS)
        if attached is not None:
            for af in attached.findall('isr:attachedFile', NS):
                cur.execute("""
                    INSERT INTO attached_files (
                        isrctn_id, file_id, name, description, download_url, is_public, mime_type, file_length, md5sum
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    isrctn_id, af.findtext('isr:id', namespaces=NS), af.findtext('isr:name', namespaces=NS),
                    af.findtext('isr:description', namespaces=NS), af.get('downloadUrl'),
                    af.findtext('isr:public', namespaces=NS) == 'true',
                    af.findtext('isr:mimeType', namespaces=NS),
                    safe_int(af.findtext('isr:length', namespaces=NS)),
                    af.findtext('isr:md5sum', namespaces=NS)
                ))

        # 10. Organizations
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

        # 11. Contacts
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
            # Fallback to retry failed ones if no pending
            cur.execute("SELECT isrctn_id FROM trial_queue WHERE retrieval_status = 'failed'")
            trials_to_fetch = [row[0] for row in cur.fetchall()]
            if not trials_to_fetch:
                print("No trials in queue to process.")
                return

        total = len(trials_to_fetch)
        success_count = 0
        error_count = 0
        
        print(f"Starting retrieval of {total} trials...")
        
        for i, isrctn_id in enumerate(trials_to_fetch, 1):
            percent = (i / total) * 100
            remaining = total - i
            
            # Print progress line
            sys.stdout.write(f"\r[{i}/{total}] ({percent:5.1f}%) | [C: {success_count} | E: {error_count} | R: {remaining}] Processing {isrctn_id}...")
            sys.stdout.flush()
            
            success, error_msg = fetch_and_store_trial(isrctn_id, conn)
            
            if success:
                success_count += 1
                status = 'completed'
            else:
                error_count += 1
                status = 'failed'
                # Print the error on a new line so it doesn't get overwritten by the next progress update
                print(f"\n[!] Error fetching {isrctn_id}: {error_msg}")
            
            # Update queue status in DB
            cur.execute("""
                UPDATE trial_queue 
                SET retrieval_status = %s, 
                    last_attempt = CURRENT_TIMESTAMP,
                    error_log = %s
                WHERE isrctn_id = %s
            """, (status, error_msg, isrctn_id))
            conn.commit()
            
        print(f"\n\nBatch processing complete!")
        print(f"Total: {total}")
        print(f"Successfully processed: {success_count}")
        print(f"Failed: {error_count}")
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
