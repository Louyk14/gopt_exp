#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/execution/operator/join/physical_sip_join.hpp"
#include "duckdb/execution/operator/join/physical_merge_sip_join.hpp"

#include <unordered_set>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>

/**
This file contains an example on how a query tree can be programmatically constructed. This is essentially hand-rolling
the binding+planning phase for one specific query.

Note that this API is currently very unstable, and is subject to change at any moment. In general, this API should not
be used currently outside of internal use cases.
**/

using namespace duckdb;
using namespace std;

void replace_all(std::string& str, const std::string& from, const std::string& to) {
    if(from.empty())
        return;
    int start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

void getStringListFromFile(string filename, int index, int count, vector<string>& slist) {
    std::fstream infile(filename, std::ios::in);
    if (!infile)
        std::cout << filename << " not found" <<  std::endl;

    string schema, data;
    std::getline(infile, schema);
    std::cout << schema << std::endl;
    char delimiter = '|';

    unordered_set<string> record;

    string result = "";
    while (std::getline(infile, data)) {
        int pos = 0;
        int last = 0;
        int indexer = 0;

        while ((pos = data.find(delimiter, last)) != std::string::npos) {
            if (index == indexer) {
                string token = data.substr(last, pos - last);
                if (record.find(token) == record.end()) {
                    slist.push_back(token);
                    record.insert(token);
                }
                break;
            }
            indexer += 1;
            last = pos + 1;
        }
        if (index == indexer) {
            string token = data.substr(last, pos - last);
            if (record.find(token) == record.end()) {
                slist.push_back(token);
                record.insert(token);
            }
        }

        if (slist.size() >= count)
            break;
    }

    infile.close();
}


void extractInfo(string& inputstr, std::vector<bool>& filter, char delimiter, string& result) {
    int pos = 0;
    int last = 0;
    int indexer = 0;
    std::string token;

    while ((pos = inputstr.find(delimiter, last)) != std::string::npos) {
        if (filter[indexer]) {
            token = inputstr.substr(last, pos - last);
            result += "\'" + token + "\'" + ',';
        }
        indexer += 1;
        last = pos + 1;
    }
    if (filter[indexer]) {
        result += "\'" + inputstr.substr(last) + "\'";
    }

    int lastindex = result.size() - 1;
    if (result[lastindex] == ',') {
        result = result.substr(0, lastindex);
    }
}

void extractInfoFile(Connection& con, string filename, string tablename, std::vector<bool>& filter) {
    std::fstream infile(filename, std::ios::in);
    string schema, data;
    std::getline(infile, schema);

    string result = "";
    while (std::getline(infile, data)) {
        result.clear();
        extractInfo(data, filter, '|', result);
        con.Query("INSERT INTO " + tablename + " VALUES (" + result + ")");
        data.clear();
    }

    infile.close();
}

void CreateGraphFromSQL(Connection& con, string schema_path="", string load_path="", string constraint_path="") {
    std::ifstream schema_file(schema_path, std::ios::in); //"../../../../schema.sql"
    if (schema_file) {
        std::stringstream buffer_schema;
        buffer_schema << schema_file.rdbuf();
        string schema_sql(buffer_schema.str());
        replace_all(schema_sql, "\n", " ");
        con.Query(schema_sql);
    }
    schema_file.close();

    std::ifstream load_file(load_path, std::ios::in);
    if (load_file) {
        std::stringstream buffer_load;
        buffer_load << load_file.rdbuf();
        string load_sql(buffer_load.str());
        replace_all(load_sql, "\n", "");
        con.Query(load_sql);
    }
    load_file.close();


    std::ifstream constraint_file(constraint_path, std::ios::in);
    if (constraint_file) {
        std::stringstream buffer_constraint;
        buffer_constraint << constraint_file.rdbuf();
        string constraint_sql(buffer_constraint.str());
        replace_all(constraint_sql, "\n", "");
        con.Query(constraint_sql);
    }
    constraint_file.close();
}

void CreateGraphFromFile(Connection & con) {
    std::vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
    for (int i = 0; i < table_names.size(); ++i) {
        con.Query("DROP TABLE " + table_names[i]);
    }

    con.Query("CREATE TABLE Person(id STRING)");
    con.Query("CREATE TABLE Forum(id STRING, title STRING)");
    con.Query("CREATE TABLE Post(id STRING)");
    con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
    con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
    con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
    con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

    // string prepath = "/Users/louyk/Desktop/dbs/duckdb/resource/sample/";
    string prepath = "../../../../dataset/ldbc/sf1/";

    std::vector<bool> filter_person{true, false, false, false, false, false, false, false, false, false};
    extractInfoFile(con, prepath + "person_0_0.csv", "Person", filter_person);

    std::vector<bool> filter_forum{true, true, false};
    extractInfoFile(con, prepath + "forum_0_0.csv", "Forum", filter_forum);

    std::vector<bool> filter_post{true, false, false, false, false, false, false, false};
    extractInfoFile(con, prepath + "post_0_0.csv", "Post", filter_post);

    std::vector<bool> filter_knows{true, true, false};
    extractInfoFile(con, prepath + "person_knows_person_0_0.csv", "Knows", filter_knows);

    std::vector<bool> filter_hasmember{true, true, false};
    extractInfoFile(con, prepath + "forum_hasMember_person_0_0.csv", "HasMember", filter_hasmember);

    std::vector<bool> filter_containerof{true, true};
    extractInfoFile(con, prepath + "forum_containerOf_post_0_0.csv", "ContainerOf", filter_containerof);

    std::vector<bool> filter_hascreator{true, true};
    extractInfoFile(con, prepath + "post_hasCreator_person_0_0.csv", "HasCreator", filter_hascreator);
}

void CreateGraph(Connection& con) {
    vector<string> table_names{"Person", "Forum", "Post", "Knows", "HasMember", "ContainerOf", "HasCreator"};
    for (int i = 0; i < table_names.size(); ++i) {
        con.Query("DROP TABLE " + table_names[i]);
    }

    con.Query("CREATE TABLE Person(id STRING)");
    con.Query("CREATE TABLE Forum(id STRING, title STRING)");
    con.Query("CREATE TABLE Post(id STRING)");
    con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
    con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
    con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
    con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

    int S_NUM = 3;
    for (int i = 0; i < S_NUM; ++i) {
        con.Query("INSERT INTO Person VALUES (\'" + to_string(i) +"\')");
        con.Query("INSERT INTO Forum VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
        con.Query("INSERT INTO Post VALUES (\'" + to_string(i) + "\')");
        con.Query("INSERT INTO HasMember VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
        con.Query("INSERT INTO ContainerOf VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
        con.Query("INSERT INTO HasCreator VALUES (\'" + to_string(i) + "\',\'" + to_string(i) + "\')");
    }

    for (int i = 0; i < S_NUM; ++i) {
        for (int j = 0; j < S_NUM; ++j) {
            if (i == j)
                continue;
            con.Query("INSERT INTO Knows VALUES (\'" + to_string(i) + "\',\'" + to_string(j) + "\')");
        }
    }
}

void buildIndex(Connection& con) {
    con.Query("CREATE UNDIRECTED RAI knows_rai ON knows (FROM k_person1id REFERENCES person.p_personid, TO k_person2id REFERENCES person.p_personid)");
    con.Query("CREATE UNDIRECTED RAI forum_person_rai ON forum_person (FROM fp_forumid REFERENCES forum.f_forumid, TO fp_personid REFERENCES person.p_personid)");
    con.Query("CREATE UNDIRECTED RAI forum_tag_rai ON forum_tag (FROM ft_forumid REFERENCES forum.f_forumid, TO ft_tagid REFERENCES tag.t_tagid)");
    con.Query("CREATE UNDIRECTED RAI person_tag_rai ON person_tag (FROM pt_personid REFERENCES person.p_personid, TO pt_tagid REFERENCES tag.t_tagid)");
    con.Query("CREATE UNDIRECTED RAI person_university_rai ON person_university (FROM pu_personid REFERENCES person.p_personid, TO pu_organisationid REFERENCES organisation.o_organisationid)");
    con.Query("CREATE UNDIRECTED RAI person_company_rai ON person_company (FROM pc_personid REFERENCES person.p_personid, TO pc_organisationid REFERENCES organisation.o_organisationid)");
    con.Query("CREATE UNDIRECTED RAI message_tag_rai ON message_tag (FROM mt_messageid REFERENCES message.m_messageid, TO mt_tagid REFERENCES tag.t_tagid)");
    //con.Query("CREATE UNDIRECTED RAI hasCreator_rai ON HasCreator (FROM postId REFERENCES Post.id, TO personId REFERENCES Person.id)");
}

unique_ptr<PhysicalTableScan> get_scan_function(ClientContext& context, string& table_name, idx_t table_index,
                                                vector<idx_t>& columns_ids, vector<TypeId>& getTypes,
                                                unordered_map<idx_t, vector<TableFilter>> table_filters = unordered_map<idx_t, vector<TableFilter>>()) {

    auto table_or_view =
            Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, "", table_name);

    auto table = (TableCatalogEntry *)table_or_view;
    auto logical_get = make_unique<LogicalGet>(table, table_index, columns_ids);
    logical_get->types = getTypes;

    auto scan_function =
            make_unique<PhysicalTableScan>(*logical_get.get(), *logical_get.get()->table, logical_get.get()->table_index,
                                           *logical_get.get()->table->storage, logical_get.get()->column_ids,
                                           move(logical_get.get()->expressions), move(table_filters));

    return scan_function;
}




void create_db_conn(DuckDB& db, Connection& con) {
    con.DisableProfiling();
    con.context->transaction.SetAutoCommit(false);
    con.context->transaction.BeginTransaction();

    // CreateGraph(con);
    // CreateGraphFromFile(con);

    // con.context->transaction.Commit();

    // con.context->transaction.SetAutoCommit(false);
    // con.context->transaction.BeginTransaction();

    buildIndex(con);

    con.context->transaction.Commit();
}

void generate_queries(string query_path, string para_path, std::vector<string>& generated_queries) {
    std::ifstream para_file(para_path, std::ios::in);

    string schema, type, data;
    std::getline(para_file, schema);
    std::cout << schema << std::endl;
    std::getline(para_file, type);
    char delimiter = '|';
    std::vector<string> slots;
    std::vector<string> data_types;

    schema += delimiter;
    string cur = "";
    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i] == delimiter) {
            cur = ":" + cur;
            slots.push_back(cur);
            cur.clear();
        }
        else {
            cur += schema[i];
        }
    }

    type += delimiter;
    cur.clear();
    for (int i = 0; i < type.size(); ++i) {
        if (type[i] == delimiter) {
            data_types.push_back(cur);
            cur.clear();
        }
        else {
            cur += type[i];
        }
    }

    std::ifstream query_file(query_path, std::ios::in);
    std::stringstream buffer;
    buffer << query_file.rdbuf();
    string query_template(buffer.str());
    replace_all(query_template, "\n", " ");

    while (std::getline(para_file, data)) {
        int pos = 0;
        int last = 0;
        int indexer = 0;
        data += "|";
        vector<string> tokenizers;

        string query_template_tmp(query_template);
        while ((pos = data.find(delimiter, last)) != std::string::npos) {
            string token = data.substr(last, pos - last);
            tokenizers.push_back(token);
            if (data_types[indexer] == "VARCHAR")
                token = "\'" + token + "\'";
            if (slots[indexer] == ":durationDays") {
                long long dur = atoll(token.c_str()) * 86400000;
                long long end_time = atoll(tokenizers[indexer - 1].c_str()) + dur;
                token = to_string(end_time);
            }
            replace_all(query_template_tmp, slots[indexer], token);
            indexer += 1;
            last = pos + 1;
        }

        generated_queries.push_back(query_template_tmp);
    }
}

string get_test_query(int index) {
    if (index == 0) {
        return "select k2.k_person2id\n"
               "   from Knows k1, Knows k2\n"
               "   where\n"
               "   k1.k_person1id = 1 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 1;";
    }
    else if (index == 1) {
        return "select\n"
               "  id,\n"
               "  p_lastname,\n"
               "  min (dist) as dist,\n"
               "  p_birthday,\n"
               "  p_creationdate,\n"
               "  p_gender,\n"
               "  p_browserused,\n"
               "  p_locationip,\n"
               "  (select string_agg(pe_email, ';') from person_email where pe_personid = id group by pe_personid) as emails,\n"
               "  (select string_agg(plang_language, ';') from person_language where plang_personid = id group by plang_personid) as languages,\n"
               "  p1.pl_name,\n"
               "  (select string_agg(o2.o_name || '|' || pu_classyear::text || '|' || p2.pl_name, ';')\n"
               "     from person_university, organisation o2, place p2\n"
               "    where pu_personid = id and pu_organisationid = o2.o_organisationid and o2.o_placeid =p2.pl_placeid\n"
               "    group by pu_personid) as university,\n"
               "  (select string_agg(o3.o_name || '|' || pc_workfrom::text  || '|' || p3.pl_name, ';')\n"
               "     from person_company, organisation o3, place p3\n"
               "    where pc_personid = id and pc_organisationid = o3.o_organisationid and o3.o_placeid =p3.pl_placeid\n"
               "    group by pc_personid) as company\n"
               "from\n"
               "    (\n"
               "    select k_person2id as id, 1 as dist from knows, person where k_person1id = 6597069767674 and p_personid = k_person2id and p_firstname = \'John\'\n"
               "    union all\n"
               "    select b.k_person2id as id, 2 as dist from knows a, knows b, person\n"
               "    where a.k_person1id = 6597069767674\n"
               "      and b.k_person1id = a.k_person2id\n"
               "      and p_personid = b.k_person2id\n"
               "      and p_firstname = \'John\'\n"
               "      and p_personid != 6597069767674\n"
               "    union all\n"
               "    select c.k_person2id as id, 3 as dist from knows a, knows b, knows c, person\n"
               "    where a.k_person1id = 6597069767674\n"
               "      and b.k_person1id = a.k_person2id\n"
               "      and b.k_person2id = c.k_person1id\n"
               "      and p_personid = c.k_person2id\n"
               "      and p_firstname = \'John\'\n"
               "      and p_personid != 6597069767674\n"
               "    ) tmp, person, place p1\n"
               "  where\n"
               "    p_personid = id and\n"
               "    p_placeid = p1.pl_placeid\n"
               "  group by id, p_lastname, p_birthday, p_creationdate, p_gender, p_browserused, p_locationip, p1.pl_name\n"
               "  order by dist, p_lastname, id\n"
               "  limit 20\n"
               ";";
    }
    else if (index == 2) {
        return "select p_personid, p_firstname, p_lastname, m_messageid, COALESCE(m_ps_imagefile, m_content), m_creationdate\n"
               "from person, message, knows\n"
               "where\n"
               "    p_personid = m_creatorid and\n"
               "    m_creationdate <= 1354060800000 and\n"
               "    k_person1id = 24189255811654 and\n"
               "    k_person2id = p_personid\n"
               "order by m_creationdate desc, m_messageid asc\n"
               "limit 20\n"
               ";";
    }
    else if (index == 3) {
        return "select p_personid, p_firstname, p_lastname, ct1, ct2, totalcount\n"
               "from\n"
               " ( select k_person2id\n"
               "   from knows\n"
               "   where\n"
               "   k_person1id = 24189255821418\n"
               "   union\n"
               "   select k2.k_person2id\n"
               "   from knows k1, knows k2\n"
               "   where\n"
               "   k1.k_person1id = 24189255821418 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 24189255821418\n"
               " ) f, person, place p1, place p2,\n"
               " (\n"
               "  select chn.m_c_creatorid, ct1, ct2, ct1 + ct2 as totalcount\n"
               "  from\n"
               "   (\n"
               "      select m_creatorid as m_c_creatorid, count(*) as ct1 from message, place\n"
               "      where\n"
               "        m_locationid = pl_placeid and pl_name = 'Mauritania' and\n"
               "        m_creationdate >= 1296518400000 and  m_creationdate < 1299369600000\n"
               "      group by m_c_creatorid\n"
               "   ) chn,\n"
               "   (\n"
               "      select m_creatorid as m_c_creatorid, count(*) as ct2 from message, place\n"
               "      where\n"
               "        m_locationid = pl_placeid and pl_name = 'Liberia' and\n"
               "        m_creationdate >= 1296518400000 and  m_creationdate < 1299369600000\n"
               "      group by m_creatorid\n"
               "   ) ind\n"
               "  where CHN.m_c_creatorid = IND.m_c_creatorid\n"
               " ) cpc\n"
               "where\n"
               "f.k_person2id = p_personid and p_placeid = p1.pl_placeid and\n"
               "p1.pl_containerplaceid = p2.pl_placeid and p2.pl_name <> 'Mauritania' and p2.pl_name <> 'Liberia' and\n"
               "f.k_person2id = cpc.m_c_creatorid\n"
               "order by totalcount desc, p_personid asc\n"
               "limit 20\n"
               ";";
    }
    else if (index == 4) {
        return "select t_name, count(*) as postCount\n"
               "from tag, message, message_tag recent, knows\n"
               "where\n"
               "    m_messageid = mt_messageid and\n"
               "    mt_tagid = t_tagid and\n"
               "    m_creatorid = k_person2id and\n"
               "    m_c_replyof IS NULL and\n"
               "    k_person1id = 2199023255952 and\n"
               "    m_creationdate >= 1335830400000 and  m_creationdate < 1339027200000 and\n"
               "    not exists (\n"
               "        select * from\n"
               "  (select distinct mt_tagid from message, message_tag, knows\n"
               "        where\n"
               "    k_person1id = 2199023255952 and\n"
               "        k_person2id = m_creatorid and\n"
               "        m_c_replyof IS NULL and\n"
               "        mt_messageid = m_messageid and\n"
               "        m_creationdate < 1335830400000) tags\n"
               "  where  tags.mt_tagid = recent.mt_tagid)\n"
               "group by t_name\n"
               "order by postCount desc, t_name asc\n"
               "limit 10\n"
               ";";
    }
    else if (index == 5) {
        return "select f_title, count(m_messageid) AS postCount\n"
               "from (\n"
               "select f_title, f_forumid, f.k_person2id\n"
               "from forum, forum_person,\n"
               " ( select k_person2id\n"
               "   from knows\n"
               "   where\n"
               "   k_person1id = 6597069768320\n"
               "   union\n"
               "   select k2.k_person2id\n"
               "   from knows k1, knows k2\n"
               "   where\n"
               "   k1.k_person1id = 6597069768320 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 6597069768320\n"
               " ) f\n"
               "where f_forumid = fp_forumid and fp_personid = f.k_person2id and\n"
               "      fp_joindate >= 0\n"
               ") tmp left join message\n"
               "on tmp.f_forumid = m_ps_forumid and m_creatorid = tmp.k_person2id\n"
               "group by f_forumid, f_title\n"
               "order by postCount desc, f_forumid asc\n"
               "limit 20\n"
               ";";
    }
    else if (index == 6) {
        return "select t_name, count(*) as postCount\n"
               "from tag, message_tag, message,\n"
               " ( select k_person2id\n"
               "   from knows\n"
               "   where\n"
               "   k_person1id = 2199023255952\n"
               "   union\n"
               "   select k2.k_person2id\n"
               "   from knows k1, knows k2\n"
               "   where\n"
               "   k1.k_person1id = 2199023255952 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 2199023255952\n"
               " ) f\n"
               "where\n"
               "m_creatorid = f.k_person2id and\n"
               "m_c_replyof IS NULL and\n"
               "m_messageid = mt_messageid and\n"
               "mt_tagid = t_tagid and\n"
               "t_name <> 'Angola' and\n"
               "exists (select * from tag, message_tag where mt_messageid = m_messageid and mt_tagid = t_tagid and t_name = 'Angola')\n"
               "group by t_name\n"
               "order by postCount desc, t_name asc\n"
               "limit 10\n"
               ";";
    }
    else if (index == 7) {
        return "select p_personid, p_firstname, p_lastname, l.l_creationdate, m_messageid,\n"
               "    COALESCE(m_ps_imagefile, m_content),\n"
               "    (l.l_creationdate - m_creationdate) / 60 as minutesLatency,\n"
               "    (case when exists (select 1 from knows where k_person1id = 6597069767242 and k_person2id = p_personid) then 0 else 1 end) as isnew\n"
               "from\n"
               "  (select l_personid, max(l_creationdate) as l_creationdate\n"
               "   from likes, message\n"
               "   where\n"
               "     m_messageid = l_messageid and\n"
               "     m_creatorid = 6597069767242\n"
               "   group by l_personid\n"
               "   order by l_creationdate desc\n"
               "   limit 20\n"
               "  ) tmp, message, person, likes as l\n"
               "where\n"
               "    p_personid = tmp.l_personid and\n"
               "    tmp.l_personid = l.l_personid and\n"
               "    tmp.l_creationdate = l.l_creationdate and\n"
               "    l.l_messageid = m_messageid\n"
               "order by l.l_creationdate desc, p_personid asc\n"
               ";";
    }
    else if (index == 8) {
        return "select p1.m_creatorid, p_firstname, p_lastname, p1.m_creationdate, p1.m_messageid, p1.m_content\n"
               "  from message p1, message p2, person\n"
               "  where\n"
               "      p1.m_c_replyof = p2.m_messageid and\n"
               "      p2.m_creatorid = 19791209300771 and\n"
               "      p_personid = p1.m_creatorid\n"
               "order by p1.m_creationdate desc, p1.m_messageid asc\n"
               "limit 20\n"
               ";";
    }
    else if (index == 9) {
        return "select p_personid, p_firstname, p_lastname,\n"
               "       m_messageid, COALESCE(m_ps_imagefile, m_content), m_creationdate\n"
               "from\n"
               "  ( select k_person2id\n"
               "    from knows\n"
               "    where k_person1id = 19791209300771\n"
               "    union\n"
               "    select k2.k_person2id\n"
               "    from knows k1, knows k2\n"
               "    where k1.k_person1id = 19791209300771\n"
               "      and k1.k_person2id = k2.k_person1id\n"
               "      and k2.k_person2id <> 19791209300771\n"
               "  ) f, person, message\n"
               "where\n"
               "  p_personid = m_creatorid and p_personid = f.k_person2id and\n"
               "  m_creationdate < 1318912191494\n"
               "order by m_creationdate desc, m_messageid asc\n"
               "limit 20\n"
               ";";
    }
    else if (index == 11) {
        return "select p_personid,p_firstname, p_lastname, o_name, pc_workfrom\n"
               "from person, person_company, organisation, place,\n"
               " ( select k_person2id\n"
               "   from knows\n"
               "   where\n"
               "   k_person1id = 6597069766659\n"
               "   union\n"
               "   select k2.k_person2id\n"
               "   from knows k1, knows k2\n"
               "   where\n"
               "   k1.k_person1id = 6597069766659 and k1.k_person2id = k2.k_person1id and k2.k_person2id <> 6597069766659\n"
               " ) f\n"
               "where\n"
               "    p_personid = f.k_person2id and\n"
               "    p_personid = pc_personid and\n"
               "    pc_organisationid = o_organisationid and\n"
               "    pc_workfrom < 19980 and\n"
               "    o_placeid = pl_placeid and\n"
               "    pl_name = 'Bosnia_and_Herzegovina'\n"
               "order by pc_workfrom asc, p_personid asc, o_name desc\n"
               "limit 10\n"
               ";";
    }
    else if (index == 12) {
        return "with recursive extended_tags as (\n"
               "    select s_subtagclassid, s_supertagclassid from tagclass_recursive\n"
               "    UNION\n"
               "    select tc.tc_tagclassid, t.s_supertagclassid from tagclass tc, extended_tags t\n"
               "        where tc.tc_subclassoftagclassid=t.s_subtagclassid\n"
               ")\n"
               "select p_personid, p_firstname, p_lastname, string_agg(distinct t_name, ';'), count(*) AS replyCount\n"
               "from person, message p1, knows, message p2, message_tag, \n"
               "    (select distinct t_tagid, t_name from tag where (t_tagclassid in (\n"
               "          select distinct s_subtagclassid from extended_tags k, tagclass\n"
               "        where tc_tagclassid = k.s_supertagclassid and tc_name = 'Criminal') \n"
               "   )) selected_tags\n"
               "where\n"
               "  k_person1id = 6597069767679 and \n"
               "  k_person2id = p_personid and \n"
               "  p_personid = p1.m_creatorid and \n"
               "  p1.m_c_replyof = p2.m_messageid and \n"
               "  p2.m_c_replyof is null and\n"
               "  p2.m_messageid = mt_messageid and \n"
               "  mt_tagid = t_tagid\n"
               "group by p_personid, p_firstname, p_lastname\n"
               "order by replyCount desc, p_personid\n"
               "limit 20\n"
               ";";
    }
}

int main(int argc, char** args) {
    int count_num = 50;
    int mode = 0;//atoi(args[1]);
    int start_query_index = 11;//atoi(args[2]);
    int end_query_index = 12;//atoi(args[3]);
    //string dataset(args[4]);
    //string suffix(args[5]);
    // vector<string> constantval_list;
    // getStringListFromFile("../../../../dataset/ldbc/sf1/person_0_0.csv", 0, count_num, constantval_list);
    // constantval_list.push_back("4398046511870");

    // string query_index = "2";
    // vector<string> generated_queries;
    // string query_path = "../../../../dataset/ldbc-merge/query/queries/interactive-complex-" + query_index + ".sql";
    // string para_path = "../../../../dataset/ldbc-merge/query/paras/generated_version/sf01/interactive_" + query_index + "_param.txt";
    // generate_queries(query_path, para_path, generated_queries);

    // std::cout << "Generate Queries Over" << std::endl;

    string schema_path = "../../../resource/schema.sql";
    string load_path = "../../../resource/load.sql";
    string constraint_path = "../../../resource/constraints.sql";
    // string schema_path = "../../../../dataset/ldbc-merge/schema.sql";
    // string load_path = "../../../../dataset/ldbc-merge/load.sql";

    DuckDB db(nullptr);
    Connection con(db);
    // create_db_conn(db, con);

    CreateGraphFromSQL(con, schema_path, load_path, constraint_path);
    create_db_conn(db, con);

    string suffix_str = "";
    //if (suffix != "-1")
    //    suffix_str += "_" + suffix;

    /*std::fstream outfile("output.txt", std::ios::out);
    for (int i = 0; i < generated_queries.size(); ++i)
        outfile << generated_queries[i] << std::endl << std::endl;
    outfile.close();*/
    for (int query_index = start_query_index; query_index < end_query_index; ++query_index) {
        if (query_index == 10)
            continue;
        string query_index_str = to_string(query_index);
        vector<string> generated_queries;
        //string query_path = "../../../../dataset/ldbc-merge/query/queries/interactive-complex-" + query_index_str + ".sql";
        //string para_path = "../../../../dataset/ldbc-merge/query/paras/generated_version/" + dataset + "/interactive_" + query_index_str + "_param.txt";
        //generate_queries(query_path, para_path, generated_queries);
        
    /*std::fstream outfile("output.txt", std::ios::out);
    for (int i = 0; i < generated_queries.size(); ++i)
        outfile << generated_queries[i] << std::endl << std::endl;
    outfile.close();
*/
        for (int i = 0; i < 1; ++i) {//generated_queries.size(); ++i) {
            con.context->transaction.SetAutoCommit(false);
            con.context->transaction.BeginTransaction();
            // std::cout << i << std::endl;
            con.context->SetPbParameters(mode, "./output.log");
            // con.context->SetPbParameters(mode, "../../../../output/" + dataset + suffix_str + "/graindb/query" + query_index_str + "." + to_string(i));
            /*auto r1 = con.Query("select string_agg(o2.o_name || '|' || pu_classyear::text || '|' || p2.pl_name, ';')\n"
                            "     from person_university, organisation o2, place p2\n"
                            "    where pu_personid = 6597069767674 and pu_organisationid = o2.o_organisationid and o2.o_placeid =p2.pl_placeid\n"
                            "    group by pu_personid");
            r1->Print();
            break;*/
            // auto result = con.Query("select count(*) from organisation limit 20;");
            // auto result = con.Query(generated_queries[i]);
            auto result = con.Query(get_test_query(1));

            //con.QueryPb(generated_queries[i]);
            /*con.QueryPb("SELECT f.title FROM "
                        "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
                        "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
                        "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'"
                        + constantval_list[i] + "\'");
            */
            //con.context->transaction.Commit();
            result->Print();
        }
    }
}
