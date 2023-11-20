#include "duckdb.hpp"
#include <fstream>
#include <chrono>

using namespace duckdb;

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
        con.Query(load_sql);
    }
    load_file.close();

    std::ifstream constraint_file(constraint_path, std::ios::in);
    if (constraint_file) {
        std::stringstream buffer_constraint;
        buffer_constraint << load_file.rdbuf();
        string constraint_sql(buffer_constraint.str());
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

void create_db_conn(DuckDB& db, Connection& con) {
    con.DisableProfiling();
    //con.context->transaction.SetAutoCommit(false);
    //con.context->transaction.BeginTransaction();

    // CreateGraph(con);
    CreateGraphFromFile(con);

    //con.context->transaction.Commit();
}

void generate_queries(string query_path, string para_path, std::vector<string>& generated_queries) {
    std::ifstream para_file(para_path, std::ios::in);

    string schema, data;
    std::getline(para_file, schema);
    std::cout << schema << std::endl;
    char delimiter = '|';
    std::vector<string> slots;

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

        string query_template_tmp(query_template);
        while ((pos = data.find(delimiter, last)) != std::string::npos) {
            string token = data.substr(last, pos - last);
            replace_all(query_template_tmp, slots[indexer], token);
            indexer += 1;
            last = pos + 1;
        }

        generated_queries.push_back(query_template_tmp);
    }
}

int main() {
    int count_num = 50;
    // vector<string> constantval_list;
    // getStringListFromFile("../../../../dataset/ldbc/sf1/person_0_0.csv", 0, count_num, constantval_list);
    // constantval_list.push_back("4398046511870");

    vector<string> generated_queries;
    string query_path = "../../../../dataset/ldbc/query/queries/interactive-complex-1.sql";
    string para_path = "../../../../dataset/ldbc/query/paras/ic1.param";
    generate_queries(query_path, para_path, generated_queries);

    std::cout << "Generate Queries Over" << std::endl;

    string schema_path = "../../../../../schema.sql";
    string load_path = "../../../../../load.sql";

    DuckDB db(nullptr);
    Connection con(db);
    // create_db_conn(db, con);

    CreateGraphFromSQL(con, schema_path, load_path);

    for (int i = 0; i < 1; ++i) {//generated_queries.size(); ++i) {
        //con.context->transaction.SetAutoCommit(false);
        //con.context->transaction.BeginTransaction();
	// std::cout << i << std::endl;
        con.context->SetPbParameters(0, "../../../../output/sf1/duckdb/query" + to_string(i) + ".log");
        auto result = con.Query("select\n"
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
                    ";");
        // con.QueryPb(generated_queries[i]);
        // con.QueryPb("SELECT f.title FROM "
        //                "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof, Post po, HasCreator hc "
        //                "WHERE p2.id = k1.id2 AND p2.id = hm.personId AND f.id = hm.forumId AND f.id = cof.forumId AND "
        //                "po.id = cof.postId AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'"
        //                + constantval_list[i] + "\'");

        //con.context->transaction.Commit();
        result->Print();
    }
}
