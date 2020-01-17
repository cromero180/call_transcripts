import subprocess
import os


class ingest:
    
    def get_rtf_files(directory, file_type):
        """Pull latest file from each sub-directory"""
        files = []
#         file_type = '.rtf'
        for filename in os.listdir(directory):
            if filename.endswith(file_type):
                files.append(directory + filename)
        return files


    def rtf_to_html(rtf, html):
        error = False
        retcode = subprocess.call(["textutil",
                                           "-convert", "html",
                                           "-output", html,
                                           rtf])
        if retcode != 0:
            print ("{}: Failed to convert " \
                "(to html format)".format(rtf))
            error = True
        return html
