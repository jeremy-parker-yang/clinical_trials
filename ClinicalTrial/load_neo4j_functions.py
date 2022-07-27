import requests
import html
import data_model

def extract_all_fields(nctid):
    '''
    generates dictionary: field names -> field values for a clinical trial defined by its nctid
    arguments:
        nctid (string): clinical trial to extract data from
    returns:
        full_trial (dictionary): all field data
    '''
    # query clinical trials API to get 
    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr='
    full_trial_query += nctid + '&min_rnk=1&max_rnk=1&fmt=xml'
    full_trial_response = requests.get(full_trial_query)
    
    # extract data: field name and field value
    full_trial_response = full_trial_response.text.split('</Field>')
    full_trial = dict()
    for block in full_trial_response:
        if '<Field Name="' in block:
            block = block.split('<Field Name="')[1]
            start = block.find('">')
            field_name = block[:start].strip()
            field_value = html.unescape(block[(start+2):])
            full_trial[field_name] = field_value.strip()
    return full_trial

def list_type(text):
    '''
    generates string containing all data to create a neo4j node
    arguments:
        text (string): raw data from clinical trials api
    returns:
        (string): string representing array for neo4j
    '''
    text = text.replace('||','\n')  # '||' represents sub-element
    text_list = text.split('|')
    clean_list = [elm.replace('\"','\\"') for elm in text_list]
    return str(clean_list)          # '|' represents new element

def data_string(full_data, node_fields, special_types = data_model.special_types):
    '''
    generates string containing all data to create a neo4j node
    arguments:
        full_data (dictionary): all fields for a clinical trial
        node_fields (string list): fields for this node type
        special_types (string set): set of fields with | || data format
    returns:
        (string): data_string
    '''
    node_data = list()
    for field_name in node_fields:
        if field_name in full_data:
            field_value = str()
            if field_name in special_types:
                field_value = list_type(full_data[field_name])
            else:
                field_value = full_data[field_name].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
                field_value = '\"' + field_value + '\"'
            node_data.append(field_name + ': ' + field_value)
    return ', '.join(node_data)

def nctid_list(condition_name):
    '''
    generates list of all clinical trials (nctid) for a given condition
    arguments:
        condition_name (string): condition to search for in clinical trials api
    returns:
        (string list): clinical trials (nctid list)
    '''
    # check total number of trials
    initial_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22'
    initial_query += condition_name.replace(' ', '+') + '%22&fields=NCTId&min_rnk=1&max_rnk=1000&fmt=csv'
    initial_response = requests.get(initial_query).text.splitlines()
    total_trials = int(initial_response[4][16:-1])
    
    # add trials to list
    trials = list()
    for trial in initial_response[11:]:
        trials.append(trial.split(',')[1][1:-1])

    # break into extra queries of 1000 trials if necessary
    for rank in range(1, total_trials//1000 + 1): 
        
        # get next 1000 trials
        extra_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22' + condition_name.replace(' ', '+')
        extra_query += '%22&fields=NCTId&min_rnk=' + str(rank*1000+1) + '&max_rnk=' + str((rank+1)*1000) + '&fmt=csv'
        extra_response = requests.get(extra_query).text.splitlines()
        
        # add trials to list
        for trial in extra_response[11:]:
            trials.append(trial.split(',')[1][1:-1])
        
    # return list of trials
    return trials



