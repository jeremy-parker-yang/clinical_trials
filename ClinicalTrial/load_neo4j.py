from neo4j import GraphDatabase
from csv import DictReader
import load_neo4j_functions
import data_model

# connect to neo4j database
connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'tgcbf'))
session = connection.session()

# iterate through list of diseases/conditions
with open('conditions_matched_short.csv', 'r') as read_obj:
    gard_matches = DictReader(read_obj)
    for gard_mapping in gard_matches:
        
        # extract data from mapping
        GARDId = gard_mapping['gard_id']
        GARD_name = gard_mapping['gard_name'].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
        CT_name = gard_mapping['disease_name']
        
        # create gard condition node
        cypher_create_gard = 'MERGE (gard:GARD{GardName: \"' + GARD_name + '\", GARDId: \"' + GARDId + '\"})'
        print(GARDId + ':\t' + GARD_name)
        response_trial_exists = session.run(cypher_create_gard)
        
        # add each clinical trial to neo4j
        trials = load_neo4j_functions.nctid_list(CT_name)
        print('\tnum trials:', len(trials))
        print('\t',end='')
        for trial in trials:
            
            # generate neo4j query to attach disease to clinical trial
            cypher_add_trial = 'MATCH (gard:GARD) WHERE gard.GardName = \'' + GARD_name + '\''
            
            # neo4j query to check if clinical trial node already exists
            cypher_trial_exists = 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \''+trial+'\' RETURN COUNT(trial)'
            response_trial_exists = session.run(cypher_trial_exists)

            # node doesn't exist, create new clinical trial node and connect
            if int([elm[0] for elm in response_trial_exists][0]) == 0:
                
                # extract data from clinical trial
                full_trial = load_neo4j_functions.extract_all_fields(trial)
                
                # create clinical trial node
                clinical_trial_data_string = load_neo4j_functions.data_string(full_trial, data_model.ClinicalTrial)
                cypher_add_trial += 'CREATE (gard)-[:clinical_trial]->(trial:ClinicalTrial{' 
                cypher_add_trial += clinical_trial_data_string
                cypher_add_trial += '})'
                if len(clinical_trial_data_string) == 0:
                    print('\n\ttrial:',trial,'has no data')
                
                # generate data for additional classes
                additional_class_data = list()
                for class_fields in data_model.additional_class_fields:
                    additional_class_data.append(load_neo4j_functions.data_string(full_trial, class_fields))
                    
                # cypher query to create and attach additional class nodes
                for i in range(len(additional_class_data)):
                    if len(additional_class_data[i]) > 0:
                        cypher_add_trial += 'CREATE (trial)-[:' + data_model.additional_class_connections[i]
                        cypher_add_trial += ']->(' + data_model.additional_class_variable_names[i] + ':'
                        cypher_add_trial += data_model.additional_class_names[i] + '{' + additional_class_data[i] +'})'
                    
                # run cypher query
                session.run(cypher_add_trial)
                print('+',end='')
            
            # if node exists
            else:
                
                # attach to gard node
                cypher_add_trial += 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \'' + trial + '\''
                cypher_add_trial += 'MERGE (gard)-[:clinical_trial]->(trial)'
                session.run(cypher_add_trial)
                print('.',end='')
                
        print()
        print()
        
