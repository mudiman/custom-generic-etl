import imaplib2
import  email
import os
from datetime import date
from email.quoprimime import decode
from elementtree.ElementTree import fromstring


from etl.lib.ThreadPool.ThreadPool import ThreadPool
from esync.dumps import get_dump_location

mail = imaplib2.IMAP4_SSL('imap.gmail.com')
mail.login('company.analytics@gmail.com', 'company123')
mail.select('INBOX', readonly=False);
static_types = {"ItemSold":1, "ItemListed":1, "ItemRevised":1, "ItemClosed":1}
total_emails = len(mail.search(None, 'All')[1][0].split())
threadpool = ThreadPool(40)

#FIXME:Need to pass on call back progress function
def get_email_uids_list():
    mails = mail.search(None, 'All')[1][0]
    mails_list = mails.split()
    mails_list.reverse()
#    latest_email_id = mails_list[len(mails_list)-1]
    return mails_list
    
def get_body(date="", **args):
    """
    This method read email and dump in to the files.
    """
    inc = 1
    
    p_rel = get_dump_location()
    date_str = date
    if not os.path.exists("%s/%s" % (p_rel, str(date_str))):
        os.makedirs("%s/%s" % (p_rel, str(date_str)))
    if not os.path.exists("%s/%s/%s" % (p_rel, str(date_str), "notifications")):
        os.makedirs("%s/%s/%s" % (p_rel, str(date_str), "notifications"))
    if not os.path.exists("%s/%s/%s" % (p_rel, str(date_str), "data")):
        os.makedirs("%s/%s/%s" % (p_rel, str(date_str), "data"))
    
    for i in get_email_uids_list():
        threadpool.add_task(get_emails_files_by_threads, i, p_rel, date_str, inc, args)
#        get_emails_files_by_threads(i,p_rel,date_str,inc,args)
        inc += 1
        pass
    threadpool.wait_completion()
              

        
def get_emails_files_by_threads(email_uid, rel_path, date_str, inc, callback=None):
    """
    pass
    """
    try:
        typ, msg_data = mail.FETCH(str(email_uid), '(RFC822)')
        msg = email.message_from_string(msg_data[0][1])
#        payload=msg1.get_payload()
#        msg1=extract_body(payload)
        message_string = str(msg)
               
        str1 = "<?xml"
        index1 = message_string.find("<?xml")
        index_end = message_string.find("</soapenv:Envelope>")
        msg1 = message_string[index1:(index_end + len("</soapenv:Envelope>"))]
        if msg["Content-Transfer-Encoding"] == "quoted-printable":
            msg1 = decode(msg1)
        index1 = msg1.find("<?xml")
        index_end = msg1.find("</soapenv:Envelope>")
        msg1 = msg1[index1:(index_end + len("</soapenv:Envelope>"))]
        itemid_start = msg1.find("<ItemID>");
        itemid_end = msg1.find("</ItemID>");
        itemid = msg1[itemid_start + 8:itemid_end]
        for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_string(response_part[1])
                    header = "Subject"
#                    if msg["From"]=="notification@ebay.com":
                    if "notification@ebay.com" in msg["From"]:
                        file_name = msg[header].split("(")[1][:-1]
                        
                        mail.store(int(email_uid), '+FLAGS', '\Deleted')
                        
                        if file_name in static_types.keys():
                            


                            with open(rel_path + "/%s/notifications/%s_%s_%s.xml" % (str(date_str), file_name, itemid, str(email_uid)), "w") as f:
                                f.write(str(msg1))
                                progress_for_inbox_reader(callback, email_uid, total_emails, inc, rel_path, date_str) 
                                print str(inc) + ":" + "%s_%s_%s.xml" % (file_name, itemid, str(email_uid))
#                            static_types[file_name] = static_types[file_name]+1
        
    except Exception, e:
        print e
                    
def extract_body(payload):
    """
    To extract the body of email
    """
    if isinstance(payload, str):
        return payload
    else:
        return '\n'.join([extract_body(part.get_payload()) for part in payload])
    

def progress_for_inbox_reader(callback, email_uid, total_no_of_emails, inc, p_rel, date_str):
    pass
    if len(callback) == 0:
        pass
    else:
        percentage = float((float(inc) / float(total_no_of_emails)) * 100)
        if inc == 1:
            with open(p_rel + "/%s/notifications/inbox_progress.txt" % (str(date_str)), "w") as f:
                                f.write("File Writing in Progress,%s out of %s has completed,%d percent.\n" % (inc, total_no_of_emails, round(percentage, 2)))
        else:
            with open(p_rel + "/%s/notifications/inbox_progress.txt" % (str(date_str)), "a") as f:
                                f.write("File Writing in Progress,%s out of %s has completed,%d percent.\n" % (inc, total_no_of_emails, round(percentage, 2)))
def logout():
    mail.expunge()
    mail.delete("INBOX")
    mail.close()
    mail.logout()
    
if __name__ == "__main__":
    
    get_body("test", callback="yes")
    logout()
    
