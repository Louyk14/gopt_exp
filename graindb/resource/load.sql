COPY forum             FROM '../../../resource/sf01/dynamic/forum_0_0.csv'                       (DELIMITER '|', HEADER);
COPY forum_person      FROM '../../../resource/sf01/dynamic/forum_hasMember_person_0_0.csv'      (DELIMITER '|', HEADER);
COPY forum_tag         FROM '../../../resource/sf01/dynamic/forum_hasTag_tag_0_0.csv'            (DELIMITER '|', HEADER);
COPY organisation      FROM '../../../resource/sf01/static/organisation_0_0.csv'                 (DELIMITER '|', HEADER);
COPY person            FROM '../../../resource/sf01/dynamic/person_0_0.csv'                      (DELIMITER '|', HEADER);
COPY person_email      FROM '../../../resource/sf01/dynamic/person_email_emailaddress_0_0.csv'   (DELIMITER '|', HEADER);
COPY person_tag        FROM '../../../resource/sf01/dynamic/person_hasInterest_tag_0_0.csv'      (DELIMITER '|', HEADER);
COPY likes             FROM '../../../resource/sf01/dynamic/person_likes_post_0_0.csv'           (DELIMITER '|', HEADER);
COPY likes             FROM '../../../resource/sf01/dynamic/person_likes_comment_0_0.csv'        (DELIMITER '|', HEADER);
COPY person_language   FROM '../../../resource/sf01/dynamic/person_speaks_language_0_0.csv'      (DELIMITER '|', HEADER);
COPY person_university FROM '../../../resource/sf01/dynamic/person_studyAt_organisation_0_0.csv' (DELIMITER '|', HEADER);
COPY person_company    FROM '../../../resource/sf01/dynamic/person_workAt_organisation_0_0.csv'  (DELIMITER '|', HEADER);
COPY place             FROM '../../../resource/sf01/static/place_0_0.csv'                        (DELIMITER '|', HEADER);
COPY tagclass          FROM '../../../resource/sf01/static/tagclass_0_0.csv'                     (DELIMITER '|', HEADER);
COPY tag               FROM '../../../resource/sf01/static/tag_0_0.csv'                          (DELIMITER '|', HEADER);


COPY post              FROM '../../../resource/sf01/dynamic/post_0_0.csv'                        (DELIMITER '|', HEADER);
COPY comment           FROM '../../../resource/sf01/dynamic/comment_0_0.csv'                     (DELIMITER '|', HEADER);

COPY post_tag          FROM '../../../resource/sf01/dynamic/post_hasTag_tag_0_0.csv'             (DELIMITER '|', HEADER);
COPY comment_tag       FROM '../../../resource/sf01/dynamic/comment_hasTag_tag_0_0.csv'          (DELIMITER '|', HEADER);

COPY knows (k_person1id, k_person2id, k_creationdate) FROM '../../../resource/sf01/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);
COPY knows (k_person2id, k_person1id, k_creationdate) FROM '../../../resource/sf01/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);


CREATE TABLE country AS
    SELECT city.pl_placeid AS ctry_city, ctry.pl_name AS ctry_name
    FROM place city, place ctry
    WHERE city.pl_containerplaceid = ctry.pl_placeid
      AND ctry.pl_type = 'country';

CREATE TABLE message_tag AS
    SELECT mt_messageid, mt_tagid FROM post_tag
    UNION ALL
    SELECT mt_messageid, mt_tagid FROM comment_tag;

CREATE TABLE message AS
    SELECT m_messageid, m_ps_imagefile, m_creationdate, m_locationip, m_browserused, m_ps_language, m_content, m_length, m_creatorid, m_locationid, m_ps_forumid, NULL AS m_c_replyof
    FROM post
    UNION ALL
    SELECT m_messageid, NULL, m_creationdate, m_locationip, m_browserused, NULL, m_content, m_length, m_creatorid, m_locationid, NULl, coalesce(m_replyof_post, m_replyof_comment)
    FROM comment;

CREATE TABLE tagclass_recursive AS
    SELECT tc_tagclassid as s_subtagclassid, tc_tagclassid as s_supertagclassid
    FROM tagclass;

DROP TABLE post;
DROP TABLE comment;
DROP TABLE post_tag;
DROP TABLE comment_tag;
