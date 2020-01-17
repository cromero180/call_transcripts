class tidy:
    
    def remove_html_tags(text):
        """Remove html tags from a string"""
        bracket = re.compile('<.*?>')
        amp = re.compile('&amp;')
        text = re.sub(bracket,'', text)
        return re.sub(amp,'&', text)


    def remove_titles_names(text):
        """Remove foreign chars from a string"""
        italics = re.compile('<i>(.*?)</i>')
        bolds = re.compile('<b>(.*?)</b>')
        text = re.sub(italics,'', text)
        return re.sub(bolds,'', text)


    def remove_foreign_chars(text):
        """Remove foreign chars from a string"""
        text = re.sub('\n','', text)
        text = re.sub('copyright','', text)
        return re.sub('\xa0','', text)
