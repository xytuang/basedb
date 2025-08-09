use basedb::common::basedb_instance::BaseDBInstance;

fn main() {
    let default_prompt = "basedb> ";
    let basedb = BaseDBInstance::new("basedb.db");
    println!("Welcome to BaseDB.\n");

    // Gets the query
    // while true {

    // }
    let query = "".to_string();
    let error = basedb.execute_sql(query);
}
