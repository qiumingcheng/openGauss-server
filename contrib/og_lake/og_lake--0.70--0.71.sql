/* SQL71 (PGL-2 / PGL-6): local JSON / JSONL COPY FROM/TO, og_lake_file_fdw,
 * external schema inference, and external_file_info. */
DO $og_lake_json_format_0_71$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'json_format_io')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'json_format_sql_version') <> '0.71',true)
       OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(og_lake.host_status()::jsonb -> 'copy_formats') v WHERE v = 'json')
       OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(og_lake.host_status()::jsonb -> 'external_file_formats') v WHERE v = 'json') THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.71 requires its reviewed JSON/JSONL I/O runtime';
    END IF;
END
$og_lake_json_format_0_71$;
