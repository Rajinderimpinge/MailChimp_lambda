"""
Script to Extract the data from MailChimp list '8725f4d734' 
and update that record in Knack Database.
"""
import pprint, json, requests, os, datetime
from dotenv import load_dotenv
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import dateutil.parser as dparser
from datetime import datetime, timedelta

pp = pprint.PrettyPrinter(indent=4)
load_dotenv()


class MailchimpScripts:
    """
    getting the today's records from mailchimp and updating in knack's databse
    """
    def __init__(self):
        self.MAILCHIMP_API_KEY = os.getenv("MAILCHIMP_API_KEY")
        self.MAILCHIMP_SERVER = os.getenv("MAILCHIMP_SERVER")
        self.KNACK_APP_ID = os.getenv("KNACK_APP_ID")
        self.KNACK_KEY = os.getenv("KNACK_KEY")

        self.client = MailchimpMarketing.Client()
        self.client.set_config(
            {"api_key": self.MAILCHIMP_API_KEY, "server": self.MAILCHIMP_SERVER}
        )

    def get_all_lists(self):
        list_ids = list()
        lists_response = self.client.lists.get_all_lists()
        for list_ in lists_response.get("lists"):
            list_ids.append(list_["id"])

        return list_ids

    def get_list_member_info(self, list_ids=None):
        count_set = 0

        list_id = "8725f4d734"
        rec_list = list()
        print("self.client.lists.get_list_members_info) :: request ::", datetime.now())
        list_info_response = self.client.lists.get_list_members_info(
                list_id, count=1000
            )
        print("self.client.lists.get_list_members_info) :: response :: ", datetime.now(), "list_info_response :: ", len(list_info_response), file=open('list_info_response.json', 'w'))
        print("self.client.lists.get_list_members_info) :: response :: ", datetime.now(), "list_info_response :: ", len(list_info_response.get("members")))
        range_ = int(int(list_info_response.get("total_items")) / 1000)
        print("get_list_member_info() :: range", range_)
        
        for i in range(0, range_ + 1):
            print('range members>>>>>>>', count_set, 'list_id>>>>>>>>>>>', list_id, "list_count>>>>>>>", len(rec_list) )
            count_set += 1000
            print("self.client.lists.get_list_members_info) :: request ::", datetime.now())
            
            list_info_response = self.client.lists.get_list_members_info(
                list_id, count=1000, offset=count_set
            )
            print("self.client.lists.get_list_members_info) :: response :: ", datetime.now(), "list_info_response :: ", len(list_info_response))
            print("self.client.lists.get_list_members_info) :: response :: ", datetime.now(), "list_info_response :: ", len(list_info_response.get("members")))

            if not list_info_response.get("members"):
                break
            else:

                for members_ in list_info_response.get("members"):
                    
                    if members_.get("status") == 'subscribed':
                        continue
                    rec_list.append(members_)
                    # print(len(rec_list))

                    # self.get_knack_one_record(members_)
                    # if (
                    #     members_.get("email_address") == "ed.miller@philips.com"
                    # ):  # :now, just for testing
                    #     self.get_knack_one_record(members_) 
        return True         

    def get_knack_one_record(self, members_info):
        print('get_knack_one_record() ::')
        try:
            tb_object = "object_14"
            email_ID = members_info.get("email_address")
            headers = {
                "X-Knack-Application-Id": self.KNACK_APP_ID,
                "X-Knack-REST-API-Key": self.KNACK_KEY,
            }
            filter_set = json.dumps(
                {
                    "match": "and",
                    "rules": [
                        {"field": "field_64", "operator": "is", "value": f"{email_ID}"}
                    ],
                }
            )
            try:
                url = f"https://api.knack.com/v1/objects/{tb_object}/records/?filters={filter_set}"
                response = requests.request("GET", url, headers=headers)
            except Exception as e:
                print("[get_knack_one_record()] :: [error] ::", e)

            knack_data = json.loads(response.text)
            if not knack_data.get("records"):
                print("knack_data.get('records') is empty")
                return True
            
            knack_record_id = knack_data.get("records")[0].get("id")

            print('get_knack_one_record() :: "status" == cleaned or unsubscribe :: ', members_info.get("status"))
            if members_info.get("status") == "cleaned":
                self.update_knack(
                    tb_object,
                    knack_record_id,
                    members_info,
                    members_info.get("status"),
                    members_info.get("last_changed"),
                    cleaned=members_info.get("last_changed"),
                )
            if members_info.get("status") == "unsubscribed":
                self.update_knack(
                    tb_object,
                    knack_record_id,
                    members_info,
                    members_info.get("status"),
                    members_info.get("last_changed"),
                    unsub=members_info.get("last_changed"),
                )

            return True
        except Exception as e:
            import traceback
            print("Error ", e)
            print("Error ", traceback.format_exc())
            breakpoint()

    def update_knack(
        self,
        tb_object,
        id,
        members_info,
        Status=None,
        Update=None,
        cleaned=None,
        unsub=None,
    ):
        url = f"https://api.knack.com/v1/objects/{tb_object}/records/{id}"

        headers = {
            "X-Knack-Application-Id": self.KNACK_APP_ID,
            "X-Knack-REST-API-Key": self.KNACK_KEY,
            "Content-Type": "application/json",
        }

        payload = self.Cardiovascular_Business(
            Status=Status, Update=Update, cleaned=cleaned, unsub=unsub
        )

        try:
            pass
            # response = requests.request("PUT", url, headers=headers, data=payload)
        except Exception as e:
            pass

        # breakpoint()

    def Cardiovascular_Business(self, Status, Update, cleaned=None, unsub=None):
        """
        CVB_Status = 'field_246'
        CVB_Update = 'field_298'
        CVB_cleaned = 'field_259'
        CVB_unsub = 'field_255'
        """
        cardiovascular_business = {
            "field_246": Status,
            "field_298": Update,
        }

        if cleaned is not None:
            cardiovascular_business["field_259"] = cleaned

        if unsub is not None:
            cardiovascular_business["field_255"] = unsub

        return json.dumps(cardiovascular_business)


def lambda_handler(event, context):
    mailchimp_obj = MailchimpScripts()
    print('lambda_handler ................')
    status = mailchimp_obj.get_list_member_info()
    if bool(status):
        return {
            'statusCode': 200,
            'body': json.dumps('I have done!')
        }
    else:
        return  {
            'statusCode': 500,
            'body': json.dumps('Something went wrong!')
        }

if __name__ == '__main__':
    event = ''
    context = ''
    lambda_handler(event, context)
