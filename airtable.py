import pandas as pd
import config
from pyairtable import Table
from pyairtable.formulas import match

## Read Ballotpedia download file for today
bp_updates = pd.read_csv('../bp/Insurrectionist Candidates Updates.csv', low_memory=False)
## Create Dataframe for BP data
bp_updates_df = pd.DataFrame(bp_updates)

##Instantiate Airtable DBs
person_table = Table(config.api_key, config.base_key, config.person_table_id)
data_sources_table = Table(config.api_key, config.base_key, config.data_source_table_id)
elected_office_table = Table(config.api_key, config.base_key, config.elected_office_table_id)

##Extract lists from Ballotpedia dataframe
name_list = bp_updates_df['Name'].values.tolist()
url_list = bp_updates_df['Ballotpedia URL'].values.tolist()
candidate_id_list = bp_updates_df['Candidate ID'].values.tolist()
candidate_status_list = bp_updates_df['Candidate status'].values.tolist()
election_date_list = bp_updates_df['Election date'].values.tolist()

## Merge data into coordinated values tuple
merged = tuple(zip(name_list,url_list,candidate_id_list,candidate_status_list,election_date_list))

## Adding Ballotpedia URLs as Data Sources
def add_bp_urls ():
    ## Searching tuple keeping index
    for idx, candidate in enumerate(merged):
        ## Search Formula
        person_search_formula = match({'Person Record ID': candidate[0]})
        person_record = person_table.all(formula=person_search_formula)

        ## Extracting variables 
        data_sources = person_record[0].get('fields').get('Data Source(s)')
        record_name = person_record[0].get('fields').get('Person Record ID')
        record_id = person_record[0].get('id')

        try:
            sources = []
            for data in data_sources:
                retrieved_record = data_sources_table.get(data)
                sourcing_org = retrieved_record.get('fields').get('Sourcing Organization')
                sources.append(sourcing_org)

            if 'Ballotpedia' in sources:
                print('Profile already has Ballotpedia URL')

            else:
                data_sources_table.create({'Source Link Name': record_name, 'Source URL': url_list[idx], 'Sourcing Organization': 'Ballotpedia', 'Person': [record_id]})
                print(record_name, ' has an updated Ballotpedia Data Source')

        except:
            print(record_name, ' has no data sources')

## Accepting a date as a parameter (YYYY-MM-DD), checks candidate status of election
def check_losses (date):
    for candidate in merged:
        elected_office_search_formula = match({'WP Persons Title': candidate[0]})

        if date in candidate:
            try:
                elected_office_record = elected_office_table.all(formula=elected_office_search_formula)
                elected_office_id = elected_office_record[0].get('id')
            except:
                print(candidate[0], 'needs a manual check')

            if 'Lost' in candidate:
                print(candidate[0], ' lost election')
               
                try:
                    elected_office_table.update(elected_office_id,{'Elected to Office' : 'No'} )

                except:
                    print('Error when updating ', candidate[0], 'that candidate lost.')
            
            if 'Withdrew' in candidate:
                print(candidate[0], ' withdrew from election')

                try:
                    elected_office_table.update(elected_office_id,{'Withdrawn?' : True} )

                except:
                    print('Error when updating ', candidate[0], 'that candidate withdrew.')
            
            else:
                print(candidate[0], candidate[3])

        if date == '2022-11-08':
            print('election day')

check_losses('2022-06-07')