def test_api_key(api_key) :
    assert api_key=="myapikey_1234"

def test_channel_handle(channel_handle) :
    assert channel_handle=="mymock_channel"
    

def test_postgres_conn(mock_postgres_conn_var)  :
    conn =mock_postgres_conn_var
    assert conn.login=="mock_username"
    assert conn.password=="mock_passwd"
    assert conn.host=="mock_myhost"
    assert conn.port==6789
    assert conn.schema=="mock_my_schema"


def test_dagbag_integrety(dagbag):
    assert dagbag.import_errors == {}, f"Import errors found : {dagbag.import_errors}"
    print("================")
    print(dagbag.import_errors)
    
    expected_dag_its=["yt_data_elt","yt_data_elt_db","yt_data_elt_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    # print(loaded_dag_ids)
    # print(loaded_dag_ids)

    for dag_id in expected_dag_its:
        assert dag_id in loaded_dag_ids, f"{dag_id} is missing from the list."
        
    assert dagbag.size() == 3
    print("=================")
    
    expected_tasks_count={
        "yt_data_elt":4,
        "yt_data_elt_db":2,
        "yt_data_elt_quality":2
    }
    
    for dag_id,dag in dagbag.dags.items() :
        expected_count=expected_tasks_count[dag_id]
        actual_count=len(dag.tasks)
        assert (expected_count==actual_count),f"Dag {dag_id} has {actual_count} tasks, expected {expected_count}"
        print(dag_id,len(dag.tasks))