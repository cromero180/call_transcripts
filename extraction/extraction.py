import re

class Extract:

    def get_section(self, text, sec_tag):
        """Stratify table of contents into list of chapters 
        by extracted hyperlinks"""

        # Set iterations based on instances of tag
        num_headers = len(re.findall(sec_tag, text, re.DOTALL))

        chapters = []
        # Loop thru text
        for s in range(num_headers):

            # Extract chapter
            match = re.search(sec_tag, text, re.DOTALL)
            chapter = match.group(0)

            # Get chapter end index
            end = text.find(chapter) + len(chapter)

            # Update chapter list
            chapters.append(chapter)

            # Reset starting point
            text = text[end:]
            s += 1

        return chapters[s-1]    


    def get_headers(self, text, name_tag):
        headers = re.findall(name_tag, text, re.DOTALL)
        return headers


    def get_names_roles(self, headers, role_types, secs):
        roles = []
        names = []
        role = ''
        for header in headers:
            if header in role_types:
                role = header
                continue
            if header not in secs:
                names.append(header)
                roles.append(role)

        return names, roles


    def get_titles(self, text, names, title_tag):
        titles = []
        for i in range(len(names)):
            if i == len(names) - 1:
                match = re.search(names[i] + '(.*?)presentation', text, re.DOTALL)
                title_area = match.group(0)
            else:
                match = re.search(names[i] + '(.*?)' + names[i+1], text, re.DOTALL)
                title_area = match.group(0)

            try:
                match = re.search(title_tag, title_area, re.DOTALL)
                titles.append(match.group(0))
            except:
                titles.append('unknown')
                continue

        return titles


    def get_presentations(self, text, names, name_tag, last_name_tag, last_section):
        comments = []
        for i in range(len(names)):
            if i == len(names) - 1:
                if last_section == False:
                    match = re.search(names[i] + '(.*?)question and answer', text, re.DOTALL)
                else:
                    match = re.search(names[i] + '(.*?)</html>', text, re.DOTALL)
                    name_tag = last_name_tag
                comment_area = match.group(0)
            else:
                match = re.search(names[i] + '(.*?)<b>', text, re.DOTALL)
                comment_area = match.group(0)

                # Get chapter end index
                end = text.find(comment_area) + len(comment_area)

                # Reset starting point
                text = text[end:]

            try:
                match = re.search(name_tag, comment_area, re.DOTALL)
                comments.append(match.group(0))
            except:
                comments.append('unknown')
                continue

        return comments
