import scrapy
import json
import math
from peewee import *
from datetime import date
import time
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import re

db = SqliteDatabase('de_campaign.db')

class DeCampaignFinanceContribution(Model):
    ContributionType = CharField(null=True)
    TransactionDate = DateField(null=True)
    Transaction_id = DecimalField(null=True)
    ContributorName = CharField(null=True)
    ContributionAmount = DecimalField(null=True)
    ContributorAddress = CharField(null=True)
    OccupationTitle = CharField(null=True)
    EmployerName = CharField(null=True)
    EmployerAddressLine1 = CharField(null=True)
    ContributorType = CharField(null=True)
    ReceivingCommitteeName=CharField(null=True)
    GabId=CharField(null=True)
    WC_ID=CharField(null=True)
    Office=CharField(null=True)
    District=CharField(null=True)
    Branch=CharField(null=True)
    FilingPeriodName=CharField(null=True)
    Fund_Type=CharField(null=True)
    Fixed_Asset=CharField(null=True)
    FileAmendedVersion=CharField(null=True)
    AddressLine1=CharField(null=True)
    City=CharField(null=True)
    State_Code=CharField(null=True)
    Zip_Code=CharField(null=True)
    Zip_Ext=CharField(null=True)
    
    class Meta:
        database = db
        table_name = 'de_campaign_finance_contributions'

class DeCampaignFinanceExpenditure(Model):
    Transaction_Id=DecimalField(null=True)
    RegistrantName=CharField(null=True)
    PayeeName=CharField(null=True)
    PayeeType=CharField(null=True)
    Address=CharField(null=True)
    TransactionDate=DateField(null=True)
    ExpensePurpose=CharField(null=True)
    ExpenseCategory=CharField(null=True)
    ExpenseMethod=CharField(null=True)
    ExpenseAmount=DecimalField(null=True)
    District=CharField(null=True)
    FilingYear=CharField(null=True)
    FilingPeriodName=CharField(null=True)
    CommitteeID=CharField(null=True)
    VendorName=CharField(null=True)
    VendorAddress=CharField(null=True)
    Comments=CharField(null=True)
    Fund_Type=CharField(null=True)
    Fixed_Asset=CharField(null=True)
    Fixed_Asset_Desc=CharField(null=True)
    Fixed_Asset_Location=CharField(null=True)
    CCF_ID=CharField(null=True)
    FileAmendedVersion=CharField(null=True)
    AddressLine1=CharField(null=True)
    AddressLine2=CharField(null=True)
    City=CharField(null=True)
    State=CharField(null=True)
    ZipCode=CharField(null=True)
    
    class Meta:
        database = db
        table_name = 'de_campaign_finance_expenditures'

db.connect()
db.create_tables([DeCampaignFinanceContribution, DeCampaignFinanceExpenditure])


class TransactionSpider(scrapy.Spider):
    name = 'transaction'
    allowed_domains = ['cfrs.elections.delaware.gov']

    def start_requests(self):
        # yield SeleniumRequest(
        #     url='https://cfrs.elections.delaware.gov/Public/ViewReceipts',
        #     callback=self.start_contributions
        # )
        yield SeleniumRequest(
            url='https://cfrs.elections.delaware.gov/Public/ViewExpenses',
            callback=self.start_expenditures
        )
    
    def start_contributions(self, response):
        self.cookies = {}
        driver = response.request.meta['driver']
        select = Select(driver.find_element(by=By.ID, value='FilingYear'))
        select.select_by_value('2022')
        time.sleep(10)
        driver.find_element(by=By.ID, value='btnSearch').click()
        cookies = driver.get_cookies()
        for cookie in cookies:
            if re.match('.*\.elections\.delaware\.gov$', cookie['domain']) is not None:
                self.cookies[cookie['name']] = cookie['value']
    
        yield scrapy.Request(
            url="https://cfrs.elections.delaware.gov/Public/_ViewReceiptsCustom?Grid-size=15&theme=vista",
            method='POST',
            body="page=1&size=15",
            cookies=self.cookies,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest'
            },
            callback=self.parse_contributions
        )
    def parse_contributions(self, response):
        total = int(json.loads(response.text)['total'])
        self.handle_contribution(response)
        for i in range(2, math.ceil(total/15)+1):
            yield scrapy.FormRequest(
                url="https://cfrs.elections.delaware.gov/Public/_ViewReceiptsCustom?Grid-size=15&theme=vista",
                formdata={
                    'page':str(i),
                    'size': '15'
                },
                cookies=self.cookies,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                callback=self.handle_contribution
            )
    def handle_contribution(self, response):
        data = json.loads(response.text)['data']
        for row in data:
            model = DeCampaignFinanceContribution(
                ContributionType=row['ContributionType'],
                TransactionDate=date.fromtimestamp(int(row['TransactionDate'][6:-5])),
                Transaction_id=row['Transaction_id'],
                ContributorName=row['ContributorName'],
                ContributionAmount=row['ContributionAmount'],
                ContributorAddress=row['ContributorAddress'],
                OccupationTitle=row['OccupationTitle'],
                EmployerName=row['EmployerName'],
                EmployerAddressLine1=row['EmployerAddressLine1'],
                ContributorType=row['ContributorType'],
                ReceivingCommitteeName=row['ReceivingCommitteeName'],
                GabId=row['GabId'],
                WC_ID=row['WC_ID'],
                Office=row['Office'],
                District=row['District'],
                Branch=row['Branch'],
                FilingPeriodName=row['FilingPeriodName'],
                Fund_Type=row['Fund_Type'],
                Fixed_Asset=row['Fixed_Asset'],
                FileAmendedVersion=row['FileAmendedVersion'],
                AddressLine1=row['AddressLine1'],
                City=row['City'],
                State_Code=row['State_Code'],
                Zip_Code=row['Zip_Code'],
                Zip_Ext=row['Zip_Ext'],
            )
            model.save()

    def start_expenditures(self, response):
        self.cookies = {}
        driver = response.request.meta['driver']
        select = Select(driver.find_element(by=By.ID, value='FilingYear'))
        select.select_by_value('2022')
        time.sleep(10)
        driver.find_element(by=By.ID, value='btnSearch').click()
        cookies = driver.get_cookies()
        for cookie in cookies:
            if re.match('.*\.elections\.delaware\.gov$', cookie['domain']) is not None:
                self.cookies[cookie['name']] = cookie['value']
        
        yield scrapy.Request(
            url="https://cfrs.elections.delaware.gov/Public/_ViewExpensesCustom?Grid-size=15&theme=vista",
            method='POST',
            body="page=1&size=15",
            cookies=self.cookies,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest'
            },
            callback=self.parse_expenditures
        )
    def parse_expenditures(self, response):
        total = int(json.loads(response.text)['total'])
        self.handle_expenditure(response)
        for i in range(2, math.ceil(total/15)+1):
            yield scrapy.FormRequest(
                url="https://cfrs.elections.delaware.gov/Public/_ViewExpensesCustom?Grid-size=15&theme=vista",
                formdata={
                    'page':str(i),
                    'size': '15'
                },
                cookies=self.cookies,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                callback=self.handle_expenditure
            )
    def handle_expenditure(self, response):
        data = json.loads(response.text)['data']
        print(data)
        for row in data:
            model = DeCampaignFinanceExpenditure(
                Transaction_Id=row['Transaction_Id'],
                RegistrantName=row['RegistrantName'],
                PayeeName=row['PayeeName'],
                PayeeType=row['PayeeType'],
                Address=row['Address'],
                TransactionDate=date.fromtimestamp(int(row['TransactionDate'][6:-5])),
                ExpensePurpose=row['ExpensePurpose'],
                ExpenseCategory=row['ExpenseCategory'],
                ExpenseMethod=row['ExpenseMethod'],
                ExpenseAmount=row['ExpenseAmount'],
                District=row['District'],
                FilingYear=row['FilingYear'],
                FilingPeriodName=row['FilingPeriodName'],
                CommitteeID=row['CommitteeID'],
                VendorName=row['VendorName'],
                VendorAddress=row['VendorAddress'],
                Comments=row['Comments'],
                Fund_Type=row['Fund_Type'],
                Fixed_Asset=row['Fixed_Asset'],
                Fixed_Asset_Desc=row['Fixed_Asset_Desc'],
                Fixed_Asset_Location=row['Fixed_Asset_Location'],
                CCF_ID=row['CCF_ID'],
                FileAmendedVersion=row['FileAmendedVersion'],
                AddressLine1=row['AddressLine1'],
                AddressLine2=row['AddressLine2'],
                City=row['City'],
                State=row['State'],
                ZipCode=row['ZipCode']
            )
            model.save()

    