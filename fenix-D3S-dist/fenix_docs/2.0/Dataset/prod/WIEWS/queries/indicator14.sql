with raw as (
    SELECT
      ref_c.iso,
      iteration,
      c.wiews_instcode as stakeholder,
      answer_freetext,
      answer_freetext_local,

      reference_id


    FROM
      answer a
      JOIN
      answer_detail ad
        ON ( a.id = answerid )
      JOIN
      ref_instab c
        ON a.orgid = c.id
      JOIN
      ref_country ref_c
        ON ref_c.country_id = a.country_id

    WHERE questionid = 11 and ad.subquestionid= 1058 and ref_c.lang = 'EN' ),

    test_14_indi as (


      SELECT

        iteration,
        iso,
        stakeholder,
        CASE
        WHEN reference_id is null or reference_id = '0'  and answer_freetext is not null THEN answer_freetext
        WHEN reference_id is null or reference_id = '0'  and answer_freetext is  null THEN  answer_freetext_local
        WHEN reference_id is not null then reference_id end as crop,
        count(*) as sum


      FROM
        raw
      GROUP BY iteration,
        iso,
        stakeholder,
        crop)



                         SELECT
                           *
                         FROM
                           test_14_indi
                         ORDER BY sum,iteration, iso, stakeholder