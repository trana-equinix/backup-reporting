
import requests
import sys
import pandas as pd
from requests.auth import HTTPBasicAuth
   
import requests
import sys
import urllib3
import pandas as pd

day_of_week = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday"
}

retention_levels = {}

map_of_clients = set()
map_of_completes = set()
headers = {
    'Authorization': 'A033Q193MYUIgPSS3DMBdA_rXJhTHXlzsqKPR_5WvTRLTXqKiYLCBKdrhtl2Jl7l'
}

def get_data():
    urllib3.disable_warnings()

    open("policy_data.txt", "w").close()
    f = open("policy_data.txt", "a")
    policy_list = []

    next_link = "https://bnhs03bkp01.wrd.bellnhs.int:1556/netbackup/config/policies/"

    while next_link:
        response = requests.get(url=next_link, headers=headers, verify=False)
        data = response.json()

        # Extract Policy Name/ID
        for policy in range(len(data["data"])):
            policyName = data["data"][policy]["id"]

            if policyName != '' and "bnhs" not in policyName.lower():
                policy_details = process_policy(policyName)
            
                if policy_details != None:
                    policy_list.extend(policy_details)

        # Check if there is a 'next' link in the response
        if 'next' in data['links']:
            print("Found next link")
            next_link = data['links']['next']['href']
        else:
            next_link = None
        
    f.write(str(policy_list))
    print(policy_list)
    df = pd.DataFrame(policy_list)
    
    sorted_df = df.sort_values('Client Name')
    sorted_data = sorted_df.to_dict('records')
    return sorted_data
    print("DF Info", df.info())

    # Convert df to csv
    df.to_csv("policy_data.csv", encoding='utf-8', index=False)

    print("Data length", len(policy_list))
    f.close()


# New File Design
# 1. Call config/policies endpoint and cycle through pages
# 2. Get policyName/id and go to config/policies/policyName
# 3. collect all necessary info required by Martin


def get_retention_level(retentionLevel):
    if retentionLevel in retention_levels:
        return retention_levels[retentionLevel]
    
    response = requests.get(url="https://bnhs03bkp01.wrd.bellnhs.int:1556/netbackup/config/retentionlevels", headers=headers, verify=False)
    data = response.json()

    # Access retention level list with current retention level index
    # valueUnitPair = data["data"][retentionLevel]["attributes"]["retentionPeriod"]
    retentionLabel = data["data"][retentionLevel]["attributes"]["retentionPeriodLabel"]
    retention_levels[retentionLevel] = retentionLabel
    print("Adding to dictionary retention levels")
    return retentionLabel

def process_policy(policyName):
    response = requests.get(url="https://bnhs03bkp01.wrd.bellnhs.int:1556/netbackup/config/policies/{}".format(policyName), headers=headers, verify=False)
    data = response.json()

    if "errorMessage" in data:
        print("Policy {} not supported...".format(policyName))
        print(data)
        return None
    print("Policy Name", policyName)
    
    schedules = data['data']['attributes']['policy']['schedules']
    backupSelections = data['data']['attributes']['policy']['backupSelections']['selections']
    rows = []

    for i in range(len(schedules)):
        copies = schedules[i]['backupCopies']['copies']
        for j in range(len(copies)):
            policy_list = []
            clients = data['data']['attributes']['policy']['clients']
            for client in clients:
                clientName = client["hostName"]
        
                policy_list.append(("Client Name", clientName))
                policy_list.append(("Backup Selections", backupSelections))

                policy_list.append(("Keyword", data['data']['attributes']['policy']['policyAttributes']['keyword']))
                
                policy_list.append(("Policy Name", data['data']['attributes']['policy']['policyName']))

                policy_list.append(("Policy Type", data['data']['attributes']['policy']['policyType']))

                policy_list.append(("Retention Period", get_retention_level(copies[j]['retentionLevel'])))
                
                policy_list.append(("Retention Level", str(copies[j]['retentionLevel'])))

                policy_list.append(("Frequency Seconds", str(schedules[i]['frequencySeconds'])))

                recurringDaysOfMonth = schedules[i]['includeDates']['recurringDaysOfMonth']
                recurringDaysOfWeek = schedules[i]['includeDates']['recurringDaysOfWeek']
                specificDates = schedules[i]['includeDates']['specificDates']

                policy_list.append(("Recurring Days Of Month", str(recurringDaysOfMonth) if recurringDaysOfMonth else "None"))
                policy_list.append(("Recurring Days Of Week", str(recurringDaysOfWeek) if recurringDaysOfWeek else "None"))
                policy_list.append(("Specific Dates", str(specificDates) if specificDates else "None"))

                policy_list.append(("Schedule Name", str(schedules[i]['scheduleName'])))

                policy_list.append(("Schedule Type", str(schedules[i]['scheduleType'])))

                startWindow = schedules[i]['startWindow']
                windows_list = []

                for window in startWindow:
                    if window['startSeconds'] != 0 or window['durationSeconds'] != 0:
                        windows_list.append(day_of_week[window['dayOfWeek']] + ": Start: " + str(window['startSeconds']) + "s, Duration: " + str(window['durationSeconds']) + "s")
                policy_list.append(("Start Window", windows_list))

                job_data = {}
                for field in policy_list:
                    # print("FIELDS: ", field)
                    job_data[field[0]] = field[1]
                rows.append(job_data)
    return rows



def process_job(parsed_job_data, job):
    try:
        client_name = job['clientName']
        if client_name == '' or "bnhs" in client_name.lower():
            return
        
        # adding scheduleType, since daily can also be full backup
        complete = job['clientName']+ '-' + job['policyName'] + '-' + job['scheduleName'] + '-' + job['scheduleType']
        if client_name in map_of_clients:
            if complete in map_of_completes:
                return
        map_of_completes.add(complete)
        map_of_clients.add(client_name)

        
        response = requests.get(url="https://bnhs03bkp01.wrd.bellnhs.int:1556/netbackup/config/policies/{}".format(job.get('policyName')), headers=headers, verify=False)
        data = response.json()
        # print("Policy " + str(job.get('policyName')) + '\n' + str(data) + '\n')
        
        # print('Clients \n' + str(data['data']['attributes']['policy']['clients']))

        schedules = data['data']['attributes']['policy']['schedules']
        
        policy_list = []

        # print("Schedules ", str(schedules) + '\n')
        for i in range(len(schedules)):
            copies = schedules[i]['backupCopies']['copies']
            for j in range(len(copies)):
                # print('Retention Level \n' + str(copies[j]['retentionLevel']))
                policy_list.append(("retentionLevel2", str(copies[j]['retentionLevel'])))
                # print('Retention Period \n' + str(copies[j]['retentionPeriod']))
                policy_list.append(("retentionPeriod", str(copies[j]['retentionPeriod'])))

            # print('Frequency Seconds \n' + str(schedules[i]['frequencySeconds']))
            policy_list.append(("frequencySeconds", str(schedules[i]['frequencySeconds'])))

            # print('Include Dates \n' + str(schedules[i]['includeDates']))
            policy_list.append(("includeDates", str(schedules[i]['includeDates'])))

            # print('Schedule Name \n' + str(schedules[i]['scheduleName']))
            policy_list.append(("scheduleName1", str(schedules[i]['scheduleName'])))
            # if str(schedules[i]['scheduleName']) != "Daily":
                # print("Schedule type: ", str(schedules[i]['scheduleName']) + '\n')

            # print('Schedule Type \n' + str(schedules[i]['scheduleType']))
            policy_list.append(("scheduleType1", str(schedules[i]['scheduleType'])))

            # print('Start Window \n' + str(schedules[i]['startWindow']))
            policy_list.append(("startWindow", str(schedules[i]['startWindow'])))

            job_data = {
                'clientName': client_name,  # Can get from clients list
                'retentionLevel': job.get('retentionLevel'),    # can get from retentionLevel and retentionPeriod
                'scheduleType': job.get('scheduleType'),    # available
                'scheduleName': job.get('scheduleName'),    # available
                'policyType': job.get('policyType'),    # available
                'policyName': job.get('policyName'),    # available
                
            }
            for field in policy_list:
                print("FIELDS: ", field)
                job_data[field[0]] = field[1]
            parsed_job_data.append(job_data)
        

    except KeyError:
        return False

