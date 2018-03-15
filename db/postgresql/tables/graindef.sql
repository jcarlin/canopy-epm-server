-- Table: model.graindef

-- DROP TABLE model.graindef;

CREATE TABLE model.graindef
(
  id SERIAL NOT NULL,
    graindef_id INTEGER,
    graindef_name text COLLATE pg_catalog."default" NOT NULL,
	graindef_dimensions text[] COLLATE pg_catalog."default" NOT NULL,
   graindef_graindef json NOT NULL,
    CONSTRAINT graindef_pkey PRIMARY KEY (id),
	UNIQUE (graindef_id, graindef_name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE model.graindef
    OWNER to canopy_db_admin;