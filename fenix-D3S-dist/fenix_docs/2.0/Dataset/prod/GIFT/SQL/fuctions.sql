-- Function: array_sort(anyarray)

-- DROP FUNCTION array_sort(anyarray);

CREATE OR REPLACE FUNCTION array_sort(anyarray)
  RETURNS anyarray AS
$BODY$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION array_sort(anyarray)
  OWNER TO fenix;


-- Function: refresh_consumption(character varying)

-- DROP FUNCTION refresh_consumption(character varying);

CREATE OR REPLACE FUNCTION refresh_consumption(v_survey_code character varying)
  RETURNS void AS
$BODY$
begin
	UPDATE STAGE.CONSUMPTION_RAW SET FOODEX2_FACET = array_sort(FOODEX2_FACET);

	DELETE FROM CONSUMPTION WHERE SURVEY_CODE = V_SURVEY_CODE;

	INSERT INTO CONSUMPTION (SURVEY_CODE, ADM0_CODE, ADM1_CODE, ADM2_CODE, ROUND, SURVEY_DAY, SUBJECT, CONSUMPTION_DATE, WEEK_DAY, EXCEPTION_DAY, CONSUMPTION_TIME, MEAL, PLACE, EAT_SEQ, FOOD_TYPE, RECIPE_CODE, RECIPE_DESCR, AMOUNT_RECIPE, CODE_INGREDIENT, INGREDIENT, FOODEX_DESCRIPTION, GROUP_CODE, SUBGROUP_CODE, FOODEX2_CODE, FOODEX2_FACET, FOOD_AMOUNT_UNPROC, FOOD_AMOUNT_PROC, ENERGY, PROTEIN, A_PROT, V_PROT, CARBOH, FAT, SAT_FAT, CALC, IRON, ZINC, VITC, THIA, RIBO, NIAC, VITB6, FOLA, VITB12, VITA, BCAROT, FIBTG, ASH, MG, PHOS, POTA, NA, CU, RETOL, VITD, VITE)
	SELECT
		V_SURVEY_CODE as SURVEY_CODE,
		max(ADM0_CODE),
		max(ADM1_CODE),
		max(ADM2_CODE),
		ROUND,
		SURVEY_DAY,
		SUBJECT,
		case when max(CONSUMPTION_DAY) is not null and max(CONSUMPTION_MONTH) is not null and max(CONSUMPTION_YEAR) is not null then to_date (max(CONSUMPTION_YEAR)||' '||to_char(max(CONSUMPTION_MONTH),'00')||' '||to_char(max(CONSUMPTION_DAY),'00'), 'YYYY MM DD') else null end,
		max(WEEK_DAY),
		max(EXCEPTION_DAY),
		case when max(CONSUMPTION_TIME_HOUR) is not null and max(CONSUMPTION_TIME_MINUTES) is not null and max(CONSUMPTION_DAY) is not null and max(CONSUMPTION_MONTH) is not null and max(CONSUMPTION_YEAR) is not null then to_timestamp (max(CONSUMPTION_YEAR)||' '||to_char(max(CONSUMPTION_MONTH),'00')||' '||to_char(max(CONSUMPTION_DAY),'00')||' '||to_char(max(CONSUMPTION_TIME_HOUR),'00')||' '||to_char(max(CONSUMPTION_TIME_MINUTES),'00'), 'YYYY MM DD HH24 MI') else null end,
		MEAL,
		max(PLACE),
		max(EAT_SEQ),
		max(FOOD_TYPE),
		RECIPE_CODE,
		max(RECIPE_DESCR),
		max(AMOUNT_RECIPE),
		max(CODE_INGREDIENT),
		max(INGREDIENT),
		max(FOODEX_DESCRIPTION),
		GROUP_CODE,
		SUBGROUP_CODE,
		C.FOODEX2_CODE,
		FOODEX2_FACET,
		sum(FOOD_AMOUNT_UNPROC),
		sum(FOOD_AMOUNT_PROC),
		sum(ENERGY),
		sum(PROTEIN),
		sum(A_PROT),
		sum(V_PROT),
		sum(CARBOH),
		sum(FAT),
		sum(SAT_FAT),
		sum(CALC),
		sum(IRON),
		sum(ZINC),
		sum(VITC),
		sum(THIA),
		sum(RIBO),
		sum(NIAC),
		sum(VITB6),
		sum(FOLA),
		sum(VITB12),
		sum(VITA),
		sum(BCAROT),
		sum(FIBTG),
		sum(ASH),
		sum(MG),
		sum(PHOS),
		sum(POTA),
		sum(NA),
		sum(CU),
		sum(RETOL),
		sum(VITD),
		sum(VITE)
	FROM STAGE.CONSUMPTION_RAW C JOIN STAGE.FOOD_GROUP FG ON (FG.FOODEX2_CODE = C.FOODEX2_CODE)
	GROUP BY SUBJECT, SURVEY_DAY, ROUND, MEAL, C.FOODEX2_CODE, FOODEX2_FACET, GROUP_CODE, SUBGROUP_CODE, RECIPE_CODE;
end $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION refresh_consumption(character varying)
  OWNER TO postgres;




-- Function: refresh_subject(character varying)

-- DROP FUNCTION refresh_subject(character varying);

CREATE OR REPLACE FUNCTION refresh_subject(v_survey_code character varying)
  RETURNS void AS
$BODY$
begin
  DELETE FROM SUBJECT WHERE SURVEY_CODE = V_SURVEY_CODE;

  INSERT INTO SUBJECT (SURVEY_CODE, ADM0_CODE, ADM1_CODE, ADM2_CODE, GPS, WEIGHTING_FACTOR, ROUND, HOUSEHOLD, SUBJECT, ENVIRONMENT, GENDER, BIRTH_DATE, AGE_YEAR, AGE_MONTH, FIRST_ANT_DATE, WEIGHT, HEIGHT, METHOD_FIRST_WEIGHT, METHOD_FIRST_HEIGHT, SECOND_ANT_DATE, SWEIGHT, SHEIGHT, METHOD_SECOND_WEIGHT, METHOD_SECOND_HEIGHT, SPECIAL_DIET, SPECIAL_CONDITION, ENERGY_INTAKE, UNOVERREP, ACTIVITY, COMMENTS)
    SELECT
      V_SURVEY_CODE as SURVEY_CODE,
      ADM0_CODE,
      ADM1_CODE,
      ADM2_CODE,
      GPS,
      WEIGHTING_FACTOR,
      ROUND,
      HOUSEHOLD,
      SUBJECT,
      ENVIRONMENT,
      GENDER,
      case when BIRTHDAY is not null and BIRTHMONTH is not null and BIRTHYEAR is not null then to_date (BIRTHYEAR||' '||to_char(BIRTHMONTH,'00')||' '||to_char(BIRTHDAY,'00'), 'YYYY MM DD') else null end,
      AGE_YEAR,
      AGE_MONTH,
      case when FIRST_ANT_DAY is not null and FIRST_ANT_MONTH is not null and FIRST_ANT_YEAR is not null then to_date (FIRST_ANT_YEAR||' '||to_char(FIRST_ANT_MONTH,'00')||' '||to_char(FIRST_ANT_DAY,'00'), 'YYYY MM DD') else null end,
      WEIGHT,
      HEIGHT,
      METHOD_FIRST_WEIGHT,
      METHOD_FIRST_HEIGHT,
      case when SECOND_ANT_DAY is not null and SECOND_ANT_MONTH is not null and SECOND_ANT_YEAR is not null then to_date (SECOND_ANT_YEAR||' '||to_char(SECOND_ANT_MONTH,'00')||' '||to_char(SECOND_ANT_DAY,'00'), 'YYYY MM DD') else null end,
      SWEIGHT,
      SHEIGHT,
      METHOD_SECOND_WEIGHT,
      METHOD_SECOND_HEIGHT,
      SPECIAL_DIET,
      SPECIAL_CONDITION,
      ENERGY_INTAKE,
      UNOVERREP,
      ACTIVITY,
      COMMENTS
    FROM STAGE.SUBJECT_RAW;
end $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;



-- Function: refresh_survey_index(character varying, integer)

-- DROP FUNCTION refresh_survey_index(character varying, integer);

CREATE OR REPLACE FUNCTION refresh_survey_index(v_survey_code character varying, v_year integer)
  RETURNS void AS
$BODY$
begin
  delete from SURVEY_INDEX_DATA where SURVEY_CODE = V_SURVEY_CODE;

  insert into SURVEY_INDEX_DATA(SURVEY_CODE, ADM0_CODE, YEAR, NATIONAL)
    select V_SURVEY_CODE, ADM0_CODE, V_YEAR, case when ADM1_CODE is not null then true else false end
    from STAGE.CONSUMPTION_RAW limit 1;

  update SURVEY_INDEX_DATA set
    FOODEX2_CODE = (select array_agg(FOODEX2_CODE) from (select distinct FOODEX2_CODE from STAGE.CONSUMPTION_RAW) codes)
    ,GENDER = (select array_agg(GENDER) from (select distinct GENDER from STAGE.SUBJECT_RAW) codes)
    ,ENVIRONMENT = (select array_agg(ENVIRONMENT) from (select distinct ENVIRONMENT from STAGE.SUBJECT_RAW) codes)
    ,SPECIAL_CONDITION = (select array_agg(SPECIAL_CONDITION) from (select distinct SPECIAL_CONDITION from STAGE.SUBJECT_RAW) codes)
    ,AGE_YEAR = (select int4range(min(AGE_YEAR)::int,max(AGE_YEAR)::int,'[]') from STAGE.SUBJECT_RAW)
    ,AGE_MONTH = (select int4range(min(AGE_MONTH)::int,max(AGE_MONTH)::int,'[]') from STAGE.SUBJECT_RAW)
  where SURVEY_CODE = V_SURVEY_CODE;
end $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;


-- Function: refresh_survey_index(character varying)

-- DROP FUNCTION refresh_survey_index(character varying);

CREATE OR REPLACE FUNCTION refresh_survey_index(v_survey_code character varying)
  RETURNS void AS
$BODY$
begin
  delete from SURVEY_INDEX_DATA where SURVEY_CODE = V_SURVEY_CODE;
  insert into SURVEY_INDEX_DATA(SURVEY_CODE) values (V_SURVEY_CODE);

  update SURVEY_INDEX_DATA set
    FOODEX2_CODE = (select array_agg(FOODEX2_CODE) from (select distinct FOODEX2_CODE from STAGE.CONSUMPTION_RAW) codes)
    ,GENDER = (select array_agg(GENDER) from (select distinct GENDER from STAGE.SUBJECT_RAW) codes)
    ,ENVIRONMENT = (select array_agg(ENVIRONMENT) from (select distinct ENVIRONMENT from STAGE.SUBJECT_RAW) codes)
    ,SPECIAL_CONDITION = (select array_agg(SPECIAL_CONDITION) from (select distinct SPECIAL_CONDITION from STAGE.SUBJECT_RAW) codes)
    ,AGE_YEAR = (select numrange(min(AGE_YEAR)::numeric,max(AGE_YEAR)::numeric,'[]') from STAGE.SUBJECT_RAW)
    ,AGE_MONTH = (select numrange(min(AGE_MONTH)::numeric,max(AGE_MONTH)::numeric,'[]') from STAGE.SUBJECT_RAW)
  where SURVEY_CODE = V_SURVEY_CODE;
end $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;



