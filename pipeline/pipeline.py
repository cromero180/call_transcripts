from datetime import datetime
import logging


directory = '../Documents/General_Work/earnings_calls/data/sample/'
file_type = '.rtf'
name_tag = '<b>(.*?)</b>'
name_tag_t = '<b>(.*?)</p>'
name_tag_2 = '</b>(.*?)<b>'
last_name_tag = '</b>(.*?)copyright'
role_types = ['executives',
                    'analysts',
                    'shareholders',
                    'attendees']
secs = ['call participants', 'presentation', 'question and answer']


logging.basicConfig(filename = directory + 'log/transcripts_pipeline.log',
                    filemode = 'a',
                    format   = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt  = '%Y-%m-%d %H:%M:%S',
                    level    = logging.DEBUG)


def convert_to_html(rtf):
    try:
        html = os.path.splitext(rtf)[0] + '.html'
        ingest.rtf_to_html(rtf, html)
        with open(html) as fp:
            call_rpt = fp.read().lower()
        return call_rpt
    except:
        logging.debug('failed to convert %s to html' % (rtf))


def extract_title_info(raw, name_tag_t):
    try:
        title_tag = '<b>(.*?)table of contents'
        title_section = extract.get_section(raw, title_tag)

        org_name = extract.get_headers(title_section,name_tag_t)[0]
        call_type = extract.get_headers(title_section,name_tag_t)[1]
        call_date = extract.get_headers(title_section,name_tag_t)[2]

        org_name = tidy.remove_html_tags(org_name)
        call_type = tidy.remove_html_tags(call_type)
        call_date = tidy.remove_html_tags(call_date)
        call_date = datetime.strptime(call_date, '%A, %B %d, %Y %I:%M %p %Z')
        return org_name, call_type, call_date
    except:
        logging.debug('title info extraction failed')
    
    
def extract_participants(raw, ids, name_tag, role_types, secs):
    try:
        call_tag = '<b>call participants</b>(.*?)<b>presentation</b>'
        call_section = extract.get_section(raw, call_tag)

        call_headers = extract.get_headers(call_section,name_tag)

        title_tag = '<i>(.*?)</i>'

        callers = extract.get_names_roles(call_headers,role_types,
                                 secs)[0]

        roles = extract.get_names_roles(call_headers,role_types,
                                 secs)[1]

        titles = extract.get_titles(call_section,callers,title_tag)
        titles = list(map(tidy.remove_html_tags, titles))

        callers = list(map(tidy.remove_html_tags, callers))
        roles = list(map(tidy.remove_html_tags, roles))

        org_name_c = np.repeat(ids[0], len(callers))
        call_type_c = np.repeat(ids[1], len(callers))
        call_date_c = np.repeat(ids[2], len(callers))

        df_caller = DataFrame({'Org':org_name_c, 
                               'Call_Type':call_type_c, 
                               'Call_Date':call_date_c, 
                               'Name':callers, 
                               'Title':titles, 
                               'Role':roles})
        return df_caller
    except:
        logging.debug('participant extraction failed')

        
def extract_presentation(raw, ids, name_tag, role_types, secs, last_name_tag):
    try:
        present_tag = '<b>presentation</b>(.*?)<b>question and answer</b>'
        present_tag_end = '>lmth/<(.*?)/<>b/<noitatneserp>b<'
        if len(extract.get_headers(raw,present_tag)) > 0:
            present_section = extract.get_section(raw, present_tag)
            present_headers = extract.get_headers(present_section,name_tag)
            last_section = False
        else:
            rev = raw[::-1]
            present_section = extract.get_section(rev, present_tag_end)
            present_section = present_section[::-1]
            present_headers = extract.get_headers(present_section,name_tag)
            last_section = True

        presenters = extract.get_names_roles(present_headers,role_types,
                                 secs)[0]

        presentations = extract.get_presentations(present_section,presenters,
                                name_tag_2,last_name_tag,last_section)
        presentations = list(map(tidy.remove_titles_names, presentations))
        presentations = list(map(tidy.remove_foreign_chars, presentations))
        presentations = list(map(tidy.remove_html_tags, presentations))

        presenters = list(map(tidy.remove_html_tags, presenters))

        org_name_p = np.repeat(ids[0], len(presenters))
        call_type_p = np.repeat(ids[1], len(presenters))
        call_date_p = np.repeat(ids[2], len(presenters))

        df_presentations = DataFrame({'Org':org_name_p, 
                                       'Call_Type':call_type_p, 
                                       'Call_Date':call_date_p, 
                                       'Presenter':presenters, 
                                       'Presentation':presentations})
        return df_presentations
    except:
        logging.debug('presentation extraction failed')

        
def extract_qanda(raw, ids, name_tag, role_types, secs, name_tag_2, last_name_tag):
    try:
        comments_tag = '>lmth/<(.*?)/<>b/<rewsna dna noitseuq>b<'
        rev = raw[::-1]
        comments_section = extract.get_section(rev,comments_tag)
        comments_section = comments_section[::-1]

        comments_headers = extract.get_headers(comments_section,name_tag)
        commentors = extract.get_names_roles(comments_headers,role_types,
                                                             secs)[0]

        comments = extract.get_presentations(comments_section,commentors,
                                name_tag_2,last_name_tag,last_section=True)
        comments = list(map(tidy.remove_titles_names, comments))
        comments = list(map(tidy.remove_foreign_chars, comments))
        comments = list(map(tidy.remove_html_tags, comments))

        commentors = list(map(tidy.remove_html_tags, commentors))

        org_name_q = np.repeat(ids[0], len(commentors))
        call_type_q = np.repeat(ids[1], len(commentors))
        call_date_q = np.repeat(ids[2], len(commentors))

        df_comments = DataFrame({'Org':org_name_q, 
                                    'Call_Type':call_type_q, 
                                    'Call_Date':call_date_q,
                                    'Commentor':commentors, 
                                    'Comments':comments})
        return df_comments
    except:
        logging.debug('Q and A extraction failed')


def append_dfs(master, dfs):
    try:
        master[0].append(dfs[0])
        master[1].append(dfs[1])
        master[2].append(dfs[2])
        return master[0], master[1], master[2]
    except:
        logging.debug('appending to master list failed')
    
    
def write_to_xcel(directory, total_results):
    try:
        writer = pd.ExcelWriter(directory + 'transcript_output.xlsx', 
                                    engine='xlsxwriter',
                                    datetime_format='m/d/yyyy h:mm AM/PM',
                                    date_format='m/d/yyyy')
        participants = pd.concat(total_results[0]).drop_duplicates()
        participants.to_excel(writer, sheet_name='Participants')
        presentations = pd.concat(total_results[1]).drop_duplicates()
        presentations.to_excel(writer, sheet_name='Presentations')
        qanda = pd.concat(total_results[2]).drop_duplicates()
        qanda.to_excel(writer, sheet_name='QandA')
        writer.save()
        logging.debug('aggregated results written to excel')
    except:
        logging.debug('write to Excel failed')

        
def write_to_xcel_no_qanda(directory, total_results):
    try:
        writer = pd.ExcelWriter(directory + 'transcript_output.xlsx', 
                                    engine='xlsxwriter',
                                    datetime_format='m/d/yyyy h:mm AM/PM',
                                    date_format='m/d/yyyy')
        participants = pd.concat(total_results[0]).drop_duplicates()
        participants.to_excel(writer, sheet_name='Participants')
        presentations = pd.concat(total_results[1]).drop_duplicates()
        presentations.to_excel(writer, sheet_name='Presentations')
        writer.save()
        logging.debug('aggregated results written to excel')
    except:
        logging.debug('write to Excel failed')
        
        
def main():
    total_participants=[]
    total_presentations=[]
    total_qanda=[]
    for filename in os.listdir(directory):
            if filename.endswith(file_type):
                try:
                    logging.debug('initiating extraction for %s' % (filename))
                    call_rpt = convert_to_html(directory + filename)
                    org_name, call_type, call_date = extract_title_info(call_rpt,name_tag_t)
                    ids = (org_name, call_type, call_date)
                    dfs = (extract_participants(call_rpt,ids,name_tag,role_types,secs),
                          extract_presentation(call_rpt,ids,name_tag,role_types,secs,last_name_tag),
                          extract_qanda(call_rpt,ids,name_tag,role_types,secs,name_tag_2,last_name_tag))
                    master = (total_participants, 
                              total_presentations, 
                              total_qanda)
                    total_results = (append_dfs(master,dfs)[0],
                                     append_dfs(master,dfs)[1],
                                     append_dfs(master,dfs)[2])
                    logging.debug('results for %s appended' % (filename))
                except:
                    continue
    if total_results[2] != [None,None,None]:
        write_to_xcel(directory, total_results)
    else:
        write_to_xcel_no_qanda(directory, total_results)
    print('total results exported')
    
main()
