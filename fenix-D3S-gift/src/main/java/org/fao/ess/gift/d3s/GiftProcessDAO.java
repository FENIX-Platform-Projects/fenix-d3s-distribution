package org.fao.ess.gift.d3s;

import org.fao.ess.gift.d3s.dto.Items;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public class GiftProcessDAO extends WDSDatasetDao {
    @Inject private DataSource dataSource;
    @Inject private DatabaseUtils databaseUtils;
    @Inject private Resources resourceService;
    @Inject private UIDUtils uidUtils;
    private boolean initialized = false;

    private static final String[] tableColumns = new String[]{ "adm0_code", "adm2_code", "gps", "weighting_factor", "household", "subject", "gender", "birth_date", "age_year", "age_month", "first_ant_date", "weight", "height", "method_first_weight", "method_first_height", "second_ant_date", "sweight", "sheight", "method_second_weight", "method_second_height", "special_diet", "special_condition", "energy_intake", "unoverrep", "activity", "education", "ethnic", "profession", "survey_day", "consumption_date", "week_day", "exception_day", "consumption_time", "meal", "place", "eat_seq", "food_type", "recipe_code", "recipe_descr", "amount_recipe", "ingredient", "group_code", "subgroup_code", "foodex2_code", "facet_a", "facet_b", "facet_c", "facet_d", "facet_e", "facet_f", "facet_g", "item", "value", "um" };
    private static final int[] tableColumnsJdbcType = new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR};
    private static final Map<String, Integer> tableColumnsIndex = new HashMap<>();
    static { for (int i = 0; i< tableColumns.length; i++) tableColumnsIndex.put(tableColumns[i], i); }


    @Override
    public boolean init() {
        return !initialized;
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
        if (!initialized)
            dataSource.init(properties.get("url"),properties.get("usr"),properties.get("psw"));
        initialized = true;
    }

    @Override
    public Iterator<Object[]> loadData(MeIdentification resource) throws Exception {
        Items item = getItem(resource.getUid());
        String survey = getSurvey(resource.getUid());
        if (survey==null || item==null)
            return null;

        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(
                buildQuery(((DSDDataset)resource.getDsd()).getColumns(), item),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
        statement.setString(1, survey);
        statement.setFetchSize(100);

        return new DataIterator(statement.executeQuery(),connection,null,null);
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    private String buildQuery(Collection<DSDColumn> columns, Items item) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns) {
            String columnId = column.getId();
            if (columnId.equalsIgnoreCase("item")) {
                columnId = '\''+item.toString()+'\''+" as item";
            } else if (columnId.equalsIgnoreCase("value")) {
                columnId = item.toString()+" as value";
            } else if (columnId.equalsIgnoreCase("um")) {
                columnId = '\''+item.getUm()+'\''+" as um";
            } else if (columnId.equalsIgnoreCase("ADM0_CODE") || columnId.equalsIgnoreCase("ADM1_CODE") || columnId.equalsIgnoreCase("ADM2_CODE") || columnId.equalsIgnoreCase("SUBJECT") || columnId.equalsIgnoreCase("SURVEY_CODE")) {
                columnId = "S."+columnId;
            } else {
                int type = tableColumnsJdbcType[tableColumnsIndex.get(columnId)];
                if (type == Types.DATE)
                    columnId = "to_number(to_char(" + columnId + ", 'YYYYMMDD'), '99999999')";
                else if (type == Types.TIMESTAMP)
                    columnId = "to_number(to_char(" + columnId + ", 'YYYYMMDDHH24MISS'), '99999999999999')";
            }
            select.append(',').append(columnId);
        }

        return "select "+select.substring(1)+" from SUBJECT S join CONSUMPTION C on (S.SURVEY_CODE = C.SURVEY_CODE and S.SUBJECT = C.SUBJECT) WHERE S.SURVEY_CODE = ?";
    }


    //Utils
    private Items getItem(String uid) {
        try {
            return Items.valueOf(uid.substring("gift_process_".length(), uid.lastIndexOf('_')));
        } catch (Exception ex) {
            return null;
        }
    }
    private String getSurvey(String uid) {
        if (uid.startsWith("gift_process_"))
            return uid.substring(uid.lastIndexOf('_')+1);
        else
            return null;
    }


}

// Query should be like
// select S.ADM0_CODE, S.ADM2_CODE, GPS, WEIGHTING_FACTOR, HOUSEHOLD, S.SUBJECT, GENDER, BIRTH_DATE, AGE_YEAR, AGE_MONTH, FIRST_ANT_DATE, WEIGHT, HEIGHT, METHOD_FIRST_WEIGHT, METHOD_FIRST_HEIGHT, SECOND_ANT_DATE, SWEIGHT, SHEIGHT, METHOD_SECOND_WEIGHT, METHOD_SECOND_HEIGHT, SPECIAL_DIET, SPECIAL_CONDITION, ENERGY_INTAKE, UNOVERREP, ACTIVITY, EDUCATION, ETHNIC, PROFESSION, SURVEY_DAY, CONSUMPTION_DATE, WEEK_DAY, EXCEPTION_DAY, CONSUMPTION_TIME, MEAL, PLACE,
// EAT_SEQ, FOOD_TYPE, RECIPE_CODE, RECIPE_DESCR, AMOUNT_RECIPE, INGREDIENT, GROUP_CODE, SUBGROUP_CODE, FOODEX2_CODE,
// FACET_A, FACET_B, FACET_C, FACET_D, FACET_E, FACET_F, FACET_G, '<<indicator>>' AS ITEM, <<indicator>> AS VALUE, 'g' AS UM
// from SUBJECT S join CONSUMPTION C on (S.SURVEY_CODE = C.SURVEY_CODE and S.SUBJECT = C.SUBJECT) WHERE S.SURVEY_CODE = ?
/*
select count(*) from
(
select subject, group_code, subgroup_code, item, avg(value) as value, um from
(
	select s.subject, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_UNPROC'::varchar as item, sum(FOOD_AMOUNT_UNPROC) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'FOOD_AMOUNT_PROC'::varchar as item, sum(FOOD_AMOUNT_PROC) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'ENERGY'::varchar as item, sum(ENERGY) as value, 'kcal'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'PROTEIN'::varchar as item, sum(PROTEIN) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'A_PROT'::varchar as item, sum(A_PROT) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'V_PROT'::varchar as item, sum(V_PROT) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'CARBOH'::varchar as item, sum(CARBOH) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'FAT'::varchar as item, sum(FAT) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'SAT_FAT'::varchar as item, sum(SAT_FAT) as value, 'g'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'CALC'::varchar as item, sum(CALC) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'IRON'::varchar as item, sum(IRON) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'ZINC'::varchar as item, sum(ZINC) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'VITC'::varchar as item, sum(VITC) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'THIA'::varchar as item, sum(THIA) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'RIBO'::varchar as item, sum(RIBO) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'NIAC'::varchar as item, sum(NIAC) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'VITB6'::varchar as item, sum(VITB6) as value, 'mg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'FOLA'::varchar as item, sum(FOLA) as value, 'microgdfe'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'VITB12'::varchar as item, sum(VITB12) as value, 'microg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'VITA'::varchar as item, sum(VITA) as value, 'micrograe'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
	union all
	select s.subject, survey_day, group_code, subgroup_code, 'BCAROT'::varchar as item, sum(BCAROT) as value, 'microg'::varchar as um
	from subject s join consumption c on (s.survey_code = c.survey_code and s.subject = c.subject)
	group by s.subject, survey_day, group_code, subgroup_code
) data_by_day
group by  subject, group_code, subgroup_code, item, um
) data_avg
 */