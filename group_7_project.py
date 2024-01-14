# Team 7: Khanh Nguyen & Ryan Heiert & Justin Selby

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import http3
import csv
from serpapi import GoogleSearch
import json
import openpyxl as oxl
import slate3k as sl
import jinja2

#Converts XLSX file to pandas dataframe
def convertXLSX():
    #Create worksheet object using xlsx file
    wbFilePath = "USCovidByStateOverTime.xlsx"
    wb = oxl.load_workbook(wbFilePath)
    ws = wb["United_States_COVID-19_Cases_an"]

    header=[]
    data=[]
    #Iterate rows of worksheet
    for row in ws.iter_rows(min_col = 0,
                  max_col = 15,
                  min_row = 1,
                  max_row = 60061):
        #If header array is empty, use this row's values as the header
        if len(header) == 0:
            for cell in row:
                header.append(cell.value)
        #otherwise, add this row's values to data
        else:
            inst=[]
            for cell in row:
                inst.append(cell.value)
            data.append(inst)

    #Create the pandas dataframe using the data and header
    df = pd.DataFrame(data, columns=header)
    df=df.drop(["conf_cases","prob_cases","pnew_case","conf_death","prob_death","pnew_death","created_at","consent_cases","consent_deaths", "new_case", "new_death"],axis=1)
    df.rename(columns={'submission_date': 'date', 'state': 'state/jurisdiction of occurrence', 'tot_cases': 'cases', 'tot_death': 'deaths'}, inplace=True)

    #add the source name feature
    df['ds_source'] = 'U.S. Department of Health and Human Services'
    print("source 1 finish")
    return(df)


#Convert PDF file to pandas dataframe
def convertPDF():
    #filepath of PDF
    pdfFileName = "COVID19DailyReport.pdf"

    #create header and data with predetermined values
    header = ["submission_date","state"]
    data = [["12-12-2022","KY"]]

    #Open PDF object
    with open(pdfFileName, "rb") as pdfFileObject:
        doc = sl.PDF(pdfFileObject)
        pagenum=0
        #Split PDF object into single values
        for page in doc:
            page=page.split("\n\n")
            #Pull necessary values (weekly cases)
            if pagenum == 0:
                valnum=0
                for val in page:
                    if valnum in [4,6,8]:
                        header.append(val)
                    if valnum in [5,7,9]:
                        data[0].append(val)
                    valnum+=1
            pagenum+=1
    # Create the pandas dataframe using the data and header
    df = pd.DataFrame(data, columns=header)
    df = df.drop("New Cases", axis=1)
    df.rename(columns={'submission_date': 'date', 'state': 'state/jurisdiction of occurrence', 'Cases': 'cases',
                       'Deaths': 'deaths'}, inplace=True)
    # add the source name feature
    df['ds_source'] = 'Cabinet For Health and Family Services'
    print("source 2 finish")
    return(df)


# define the request function for the first csv file
def pull_csv1():
    # read in the csv file
    wb1 = pd.read_csv('AH_Provisional_COVID-19_Death_Counts_by_Week__Race__and_Age__United_States_2020-2022.csv')

    # drop the columns that are not  of interest in project
    wb1 = wb1.drop(columns={'Race and Hispanic Origin Group', 'Age Group', 'Total Deaths'}, axis=1)

    # rename column for merge and readability
    wb1 = wb1.rename(columns={'COVID-19 Deaths': 'Deaths (per week)'})

    # drop any duplicate values
    wb1.drop_duplicates(inplace=True)

    # replace all "0" with np.NaN
    wb1 = wb1.replace(to_replace='0', value=np.NaN)

    # convert the date to a datetime, allowing use to groupby
    wb1['Start Date'] = pd.to_datetime(wb1['Start Date'])

    # combine data that shares the same data presented in different rows
    wb1c = (wb1.groupby(['Data As Of', pd.Grouper(key='Start Date', freq='D')]).last().reindex()).replace([None],
                                                                                                          [np.NaN])
    wb1c = wb1c.drop('Year', axis=1)
    wb1c = wb1c.drop('Week-Ending Date', axis=1)
    wb1c.rename(columns={'End Date': 'date', 'Jurisdiction of Occurrence': 'state/jurisdiction of occurrence', 'MMWR Week': 'cases',
                       'Deaths (per week)': 'deaths'}, inplace=True)

    # add the source name feature
    wb1c['ds_source'] = 'Centers for Disease Control and Prevention'

    print("source 3 finish")
    # return dataframe to part1 function
    return(wb1c)


# define the request function for the second csv file
def pull_csv2():
    # read in the csv file
    wb2 = pd.read_csv('Weekly_Provisional_Counts_of_Deaths_by_State_and_Select_Causes__2020-2022.csv')

    # drop columns that are unnecessary for this data
    wb2 = wb2.drop(columns={'Septicemia (A40-A41)', 'Malignant neoplasms (C00-C97)', 'Diabetes mellitus (E10-E14)',
                            'Influenza and pneumonia (J09-J18)', 'Alzheimer disease (G30)',
                            'Chronic lower respiratory diseases (J40-J47)',
                            'Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)',
                            'Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)',
                            'Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)',
                            'Diseases of heart (I00-I09,I11,I13,I20-I51)', 'Cerebrovascular diseases (I60-I69)',
                            'flag_allcause',
                            'flag_alz', 'flag_clrd', 'flag_diab', 'flag_hd', 'flag_inflpn', 'flag_natcause',
                            'flag_neopl',
                            'flag_nephr', 'flag_otherresp', 'flag_otherunk', 'flag_sept', 'flag_stroke',
                            'flag_cov19mcod',
                            'flag_cov19ucod', 'COVID-19 (U071, Multiple Cause of Death)', 'Natural Cause'})

    # replace any value of '0' with the value 'NaN'
    wb2 = wb2.replace(to_replace="0", value=np.NaN)

    # drop all data that has no values
    wb2.dropna(inplace=True)

    # drop any data that is duplicated to not have inconsistencies
    wb2.drop_duplicates(inplace=True)

    # rename column for merge and readability
    wb2 = wb2.rename(columns={'COVID-19 (U071, Underlying Cause of Death)': 'Deaths (per week)'})

    # wb2 = wb2.drop('Week-Ending Date', axis=1)
    wb2 = wb2.drop('Data As Of', axis=1)
    wb2 = wb2.drop('MMWR Year', axis=1)
    wb2 = wb2.drop('MMWR Week', axis=1)
    wb2.rename(columns={'Week Ending Date': 'date', 'Jurisdiction of Occurrence': 'state/jurisdiction of occurrence',
                         'All Cause': 'cases',
                         'Deaths (per week)': 'deaths'}, inplace=True)

    # add the source name feature
    wb2['ds_source'] = 'Centers for Disease Control and Prevention'

    print("source 4 finish")
    # return dataframe to part1 function
    return(wb2)


# establish url to request from
js_api_url = 'https://api.covidtracking.com/v1/us/daily.json'

# define the request function to pull data from this api
def pull_api(js_api_url):
    api_data = requests.get(js_api_url).text
    api_data_text = json.loads(api_data)

    # create the data list and loop through the api
    api_cov_data = []
    for line in api_data_text:
        api_cov_data.append([line['date'], line['states'], line['positive'], line['negative'], line['pending'],
                             line['hospitalizedCurrently'], line['hospitalizedCumulative'], line['dateChecked'],
                             line['death'], line['totalTestResults'], line['lastModified']])

        # create the dataframe storing covid data pulled form api
        api_cov_df = pd.DataFrame(data=api_cov_data, columns=['Data', 'States', 'Positive', 'Negative', 'Pending',
                                                              'Hospitalized (Currently)', 'Hospitalized (Cumulative)',
                                                              'Date Checked', 'Death', 'Total Test Results',
                                                              'Last Modified'])

        api_cov_df.to_csv("API_Covid.csv")

        # read csv into a df
        wb3 = pd.read_csv("API_Covid.csv")

        # drop unnecessary columns
        wb3 = wb3.drop(['Unnamed: 0', 'Date Checked', 'Data', 'Hospitalized (Currently)'], axis=1)

        # replace all '0' with Na values.
        wb3 = wb3.replace(to_replace="0", value=np.NaN)

        # drop the Na values that skew the data
        wb3.dropna(inplace=True)

        # format the floats with commas for readability
        wb3.head().style.format("{:,.0f}")

        # drop any duplicate values that would skew data
        wb3.drop_duplicates(inplace=True)

        wb3 = wb3.drop('Total Test Results', axis=1)
        wb3 = wb3.drop('Negative', axis=1)
        wb3 = wb3.drop('Pending', axis=1)
        wb3 = wb3.drop('Hospitalized (Cumulative)', axis=1)

        # rename column for merge and readability
        wb3 = wb3.rename(columns={'Death': 'deaths',
                                  'Last Modified': 'date',
                                  'Positive': 'cases',
                                  'States': 'state/jurisdiction of occurrence'})

        # sort by last modified date
        wb3s = wb3.sort_values(by='date')

        # convert to a cleaned csv.
        wb3s.to_csv("Final_Cleaned_Source_3.csv", index=False)

        # add the source name feature
        wb3s['ds_source'] = 'The Covid Tracking Project'


        print("source 5 finish")
        # return dataframe to part1 function
        return(wb3s)

#function to merge all 5 sources
def merge(df1, df2, df3, df4, df5):
    df = pd.concat([df1, df2, df3, df4, df5], axis=0)
    return df

#remove time in date feature
def removeTime(string):
    newStr = (str(string)).split(" ")[0]
    return newStr

#pull dataframes from each of the 5 sources and merge them into a csv
def part1():
    df = merge(convertXLSX(), convertPDF(), pull_csv1(), pull_csv2(), pull_api(js_api_url))
    df['date'] = df['date'].apply(lambda row: removeTime(row))
    df = df.sort_values('date')
    #fix the index problem
    df.index = range(1, len(df) + 1)
    df.to_csv('group_7_covid.csv')
    print("5 sources are merged successfully!")



'''

3 sources are TheLadders, CareerBuilder, and GoogleJobs. We read robots.txt file of each site, and they are allowed for scapping
For the GoogleJobs, we use Serpapi of Google to take the data. The rest are scraped manually.
"Not Given": the source does not publish this feature
"Cannot Extracted": the source does not publish this feature or our team cannot take this value.
We try our best to take the minimum qualifications and desired qualifications since they are not in the same structure
so there are some values that cannot be taken.

!!! TheLadders does not prohibit to scrape but limit the number of user accessing its website. Because my program
access 1 times for the posts search and 25 more times for accesing each post to take the description so there are 26
in total which is not exceed the allowed number. But if you run the program many times for fixing errors or for another reasons,
the website will ban you from taking information. My way of solving this is I change my IP address everytime I run the program
(or run each source separately, change IP only when you test theLadders)

!!! Serpapi only allow you to take 100 requests, exceed that then you have to buy.

'''

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:105.0) Gecko/20100101 Firefox/105.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

def getJobsFromTheLadders():
    url = 'https://www.theladders.com/jobs/searchresults-jobs?keywords=data%20science&location=Newport,%20KY&order=SCORE&daysPublished=1&distance=80&remoteFlags=Remote&remoteFlags=Hybrid&remoteFlags=In-Person'
    webPage = http3.get(url, headers=headers)
    page = BeautifulSoup(webPage.content, 'html.parser')

    list = page.find('div', {'class': 'job-list-container'})
    jobs = list.find('div', {'class': 'job-list-pagination-jobs'})

    finalData = []
    if jobs is not None:
        for eachCard in jobs.findAll('div', {'class': 'job-list-pagination-job-card-container'}):
            card = []
            container = eachCard.find('div', {'class': 'job-card-text-container'})
            titleA = container.find('a', {'class': 'clipped-text'})
            title = titleA.text
            descriptionPageParams = titleA['href']

            salaryDiv = container.find('div', {'class': 'job-card-salary-label'})
            salary = salaryDiv.text

            locationContainer = container.find('div', {'class': 'job-location-container'})
            companyA = locationContainer.find('a', {'class': 'nested-anchor-link default-text'})
            company = companyA.text
            locationA = locationContainer.find('a', {'class': 'job-card-location'})
            if (locationA is not None):
                location = locationA.text
            else:
                location = 'Remotely'
            locationArr = location.split(',')
            if (len(locationArr) == 1):
                city = 'Remotely'
                state = 'Remotely'
            else:
                city = locationArr[0].strip()
                state = locationArr[1].strip()

            descriptionPageURL = 'https://www.theladders.com' + str(descriptionPageParams)

            desriptionWebPage = http3.get(descriptionPageURL, headers=headers)
            descriptionPage = BeautifulSoup(desriptionWebPage, 'html.parser')

            descriptionScript = descriptionPage.find('script', {'type': 'application/ld+json'})

            #take qualifications from the description by finding by keywords
            if (descriptionScript is not None):
                descriptionStr = descriptionScript.text

                jsonDescription = json.loads(descriptionStr)
                description = jsonDescription['description']

                splitStr = '"&lt;/li&gt;&lt;/ul&gt;&lt;p&gt;&lt;br&gt;&lt;/p&gt;&lt;p&gt;"'
                minQualificationKeywordList = ['required qualification' or 'qualifications', 'minimum qualification',
                                               'qualifications', 'minimum requirements',
                                               'minimum experience requirements',
                                               'required education and experience', 'essential requirements',
                                               'experience and skills', 'requirement']
                minQ = findQualification(minQualificationKeywordList, description, splitStr)

                desiredQualificationKeywordList = ['preferred requirements', 'preferred qualifications', 'bonus points',
                                                   'preferred skills', 'preferred job qualifications',
                                                   'other knowledge, experience or skills recommended',
                                                   'experience or skills recommended',
                                                   'preferred experience and skills',
                                                   'desirable', 'preferred']
                desiredQ = findQualification(desiredQualificationKeywordList, description, splitStr)

                card.append(title)
                card.append(company)
                card.append(salary)
                card.append(city)
                card.append(state)
                card.append('Note Given')
                card.append(minQ)
                card.append(desiredQ)

                for i in range(len(skills)):
                    card.append(checkSkills(skills[i], minQ, desiredQ))

                finalData.append(card)

    headings = ['job title', 'company name', 'salary', 'city', 'state', 'job type', 'minimum qualification', 'desired qualification', 'Degree in Data Science or relevant field', 'Cloud', 'Python', 'SQL/PostgreSQL', 'Kubernetes', 'AWS', 'Tableau']
    #write to new csv file
    with open('group_7_dsc_jobs.csv', 'w') as wFile:
        writer = csv.writer(wFile)
        writer.writerow(headings)
        for row in finalData:
            writer.writerow(row)


def getJobsFromCareerBuilder():
    finalData = []
    for page in range(3):
        #we change to URL to take jobs in all 3 pages
        url = 'https://www.careerbuilder.com/jobs?cb_apply=false&cb_veterans=false&cb_workhome=false&emp=&keywords=data+science&location=newport%2C+ky&page_number=' + str(
            page + 1) + '&pay=&posted=1&radius=30'
        webPage = http3.get(url, headers=headers)
        page = BeautifulSoup(webPage.content, 'html.parser')

        title = []
        company = []
        location = []
        city = []
        state = []
        type = []
        for jobTitle in page.findAll('div', {'class': 'data-results-title dark-blue-text b'}):
            title.append(jobTitle.text)
        for detail in page.findAll('div', {'class': 'data-details'}):
            details = []
            for span in detail.findAll('span'):
                details.append(span.text)
            if (len(details) == 2):
                company.append(details[0])
                location.append("Not Given")
                type.append(details[1])
            elif (len(details) == 3):
                company.append(details[0])
                location.append(details[1])
                type.append(details[2])

        for i in range(len(location)):
            if (location[i] == 'Work from Home/Remote'):
                city.append(location[i])
                state.append(location[i])
            elif ("," in location[i]):
                locationArr = location[i].split(",")
                city.append(locationArr[0].strip())
                state.append((locationArr[1].strip())[:2])
            else:
                city.append('Not Given')
                state.append('Not Given')

        postId = []
        aTags = page.findAll('a', {'class': 'data-results-content block job-listing-item'})
        for aTag in aTags:
            if (aTag is not None):
                jobId = aTag['data-job-did']
                postId.append(jobId)

        #we have to go to each post by using its jobId to find the URL to take the description which has the qualifications inside
        for i in range(min(len(title), len(company), len(city), len(type))):
            postURL = 'https://www.careerbuilder.com/job/' + str(postId[i])
            desriptionWebPage = http3.get(postURL, headers=headers)
            descriptionPage = BeautifulSoup(desriptionWebPage, 'html.parser')
            summary = descriptionPage.find('div', {'class': 'col big col-mobile-full jdp-left-content'})

            data = []
            minQualificationKeywordList = ['required qualification' or 'qualifications', 'minimum qualification',
                                           'qualifications', 'minimum requirements', 'minimum experience requirements',
                                           'required education and experience', 'essential requirements',
                                           'experience and skills', 'requirement']
            minQ = findQualification(minQualificationKeywordList, summary.text, '\n\n')

            desiredQualificationKeywordList = ['preferred requirements', 'preferred qualifications', 'bonus points',
                                               'preferred skills', 'preferred job qualifications',
                                               'other knowledge, experience or skills recommended',
                                               'experience or skills recommended', 'preferred experience and skills',
                                               'desirable', 'preferred']
            desiredQ = findQualification(desiredQualificationKeywordList, summary.text, '\n\n')

            data.append(title[i])
            data.append(company[i])
            data.append('Not Given')
            data.append(city[i])
            data.append(state[i])
            data.append(type[i])
            data.append(minQ)
            data.append(desiredQ)
            for i in range(len(skills)):
                data.append(checkSkills(skills[i], minQ, desiredQ))
            finalData.append(data)
    #append data to exist file
    with open('group_7_dsc_jobs.csv', 'a') as wFile:
        writer = csv.writer(wFile)
        for row in finalData:
            writer.writerow(row)


def getJobsFromGoogle():
    finalData = []
    #we take 10 pages by setting start params to run in loop of 10
    for i in range(10):
        #this is my personal api key
        params = {
            "engine": "google_jobs",
            "q": "data science",
            'start': '{}'.format(i),
            "hl": "en",
            "api_key": "84f6abb505b169c3197ff7692621b57629b4a9d210103e2c7eb356c54b30737f"
        }
        search = GoogleSearch(params)
        results = search.get_dict()

        if ("jobs_results" in results):
            jobs_results = results['jobs_results']
            for i in range(len(jobs_results)):
                card = []
                title = jobs_results[i]['title']
                card.append(title)

                company = jobs_results[i]['company_name']
                card.append(company)

                #salary is not given in this site
                salary = 'Not Given'
                card.append(salary)

                location = jobs_results[i]['location']
                if ("," in location):
                    locationArr = location.split(",")
                    city = locationArr[0]
                    state = locationArr[1]
                else:
                    city = location
                    state = location
                card.append(city)
                card.append(state)

                #description is used to extract qualifications from
                description = jobs_results[i]['description']

                minQualificationKeywordList = ['required qualification' or 'qualifications', 'minimum qualification',
                                               'qualifications', 'minimum requirements',
                                               'minimum experience requirements',
                                               'required education and experience', 'essential requirements',
                                               'experience and skills', 'requirement']
                minQ = findQualification(minQualificationKeywordList, description, '\n\n')

                desiredQualificationKeywordList = ['preferred requirements', 'preferred qualifications', 'bonus points',
                                                   'preferred skills', 'preferred job qualifications',
                                                   'other knowledge, experience or skills recommended',
                                                   'experience or skills recommended',
                                                   'preferred experience and skills',
                                                   'desirable', 'preferred']
                desiredQ = findQualification(desiredQualificationKeywordList, description, '\n\n')


                extension = jobs_results[i]['detected_extensions']
                if ('schedule_type' in extension):
                    type = extension['schedule_type']
                else:
                    type = 'Not Given'

                card.append(type)
                card.append(minQ)
                card.append(desiredQ)
                for i in range(len(skills)):
                    card.append(checkSkills(skills[i], minQ, desiredQ))
                finalData.append(card)

    #append data to exist file
    with open('group_7_dsc_jobs.csv', 'a') as wFile:
        writer = csv.writer(wFile)
        for row in finalData:
            writer.writerow(row)


'''we didn't take the writeToCSV function outside because our code somehow run slower when this function is outside.
'''
# def writeToCSV(data2, data3):
#     headers = ['job title', 'company name', 'city', 'state', 'salary', 'type', 'minimum qualification', 'desired qualification']
#     with open('FinalResult2.csv', 'w') as wFile:
#         writer = csv.writer(wFile)
#         writer.writerow(headers)
#         # for row in data1:
#         #     if (row is not None):
#         #         writer.writerow(row)
#         for row in data2:
#             if (row is not None):
#                 writer.writerow(row)
#         for row in data3:
#             if (row is not None):
#                 writer.writerow(row)

#findQualification to used to extracted minimum and desired qualifications by using keywords
def findQualification(list, summary, splitStr):
    count = 0
    for i in range(len(list)):
        qualificationIdx = summary.lower().find(list[i])
        if (qualificationIdx != -1):
            count += 1
            qualifications = summary[qualificationIdx:].partition(splitStr)[0]
            return qualifications
    return "Cannot Extracted"

skills = ['Degree', 'Cloud', 'Python', 'SQL/PostgreSQL', 'Kubernetes',
              'AWS', 'Tableau']

def checkSkills(skills, minQ, desiredQ):
    booleanVal = False
    if (skills in minQ or skills in desiredQ):
        booleanVal = True
        return booleanVal
    else:
        return booleanVal

def part2():
    # # LaddersData = getJobsFromTheLadders()
    # CareerBuilderData = getJobsFromCareerBuilder()
    # GoogleData = getJobsFromGoogle()
    # writeToCSV(CareerBuilderData, GoogleData)
    getJobsFromTheLadders()
    getJobsFromCareerBuilder()
    getJobsFromGoogle()

#Asks user which part to run and runs that function accordingly
menu=0
while menu not in [1,2]:
    menu = int(input("Which part would you like to run?   [1] Covid Data     [2] Jobs Data   "))
    if menu == 1:
        part1()
    if menu == 2:
        part2()

