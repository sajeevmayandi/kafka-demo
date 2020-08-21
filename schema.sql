DROP TABLE IF EXISTS kafka_data;
CREATE TABLE kafka_data (
               msg text,
                created_on timestamp with time zone DEFAULT now()
);
ALTER TABLE kafka_data OWNER to avnadmin;

DROP TABLE IF EXISTS current_realtime_data;
CREATE TABLE current_realtime_data
(
    telematic_id text NOT NULL,
    Latitude text  NOT NULL,
    Longitude text  NOT NULL,
    created_on timestamp with time zone NOT NULL DEFAULT now(),
    updated_on timestamp with time zone,
    CONSTRAINT current_realtime_data_pkey PRIMARY KEY (telematic_id),
    CONSTRAINT crd_unit UNIQUE (telematic_id)
);

ALTER TABLE current_realtime_data
    OWNER to avnadmin;
    CREATE UNIQUE INDEX idx_current_realtime_un1
    ON current_realtime_data USING btree
    (telematic_id, created_on);
    


CREATE OR REPLACE FUNCTION real_time_data()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100.0
AS $BODY$

	BEGIN

      INSERT INTO current_realtime_data (telematic_id, 
                                          latitude, 
                                          longitude,
                                          created_on)
      VALUES ((select (t.gps::json->0)->>'telematicid' 
                 from ( select NEW.msg::json->>'GPS' as gps ) as t), 
              (select (t.gps::json->2)->>'Latitude'  
                 from (select NEW.msg::json->>'GPS' as gps ) as t),
              (select (t.gps::json->1)->>'Longitude'  
                 from ( select NEW.msg::json->>'GPS' as gps) as t),
                   NOW())
      ON CONFLICT ON CONSTRAINT current_realtime_data_pkey
      DO UPDATE
         SET telematic_id = (select (t.gps::json->0)->>'telematicid'
                 from (select NEW.msg::json->>'GPS' as gps ) as t),
            Longitude = (select (t.gps::json->1)->>'Longitude' 
                 from (select NEW.msg::json->>'GPS' as gps ) as t),
            Latitude = (select (t.gps::json->2)->>'Latitude' 
                 from (select NEW.msg::json->>'GPS' as gps ) as t),
            updated_on = NOW();

		RETURN NULL;
	END;

$BODY$;
ALTER FUNCTION real_time_data()
    OWNER TO avnadmin;
DROP TRIGGER  IF EXISTS tr_insert_kafka_data on kafka_data;                      
CREATE TRIGGER tr_insert_kafka_data
    AFTER INSERT
    ON kafka_data
    FOR EACH ROW
    EXECUTE PROCEDURE real_time_data();

CREATE MATERIALIZED VIEW IF NOT EXISTS telematic_details AS
  select count((t.gps::json->0)->>'telematicid') as total_no_of_gpsfixes,
                      (t.gps::json->0)->>'telematicid' as telematic_id,
                      cr.updated_on as last_reported_date
                 from ( select msg::json->>'GPS' as gps  from kafka_data) as t,
                       current_realtime_data cr where cr.telematic_id = (t.gps::json->0)->>'telematicid'
                   group by (t.gps::json->0)->>'telematicid',cr.updated_on;