import subprocess
import os


class Ingest:
    
    def get_rtf_files(self, directory, file_type):
        """Pull latest file from each sub-directory"""
        files = []
        for filename in os.listdir(directory):
            if filename.endswith(file_type):
                files.append(directory + filename)
        return files

    def rtf_to_html(self, rtf, html):
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
