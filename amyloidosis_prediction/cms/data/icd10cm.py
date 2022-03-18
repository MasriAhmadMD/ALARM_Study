"""
Read out icd10 codes and descriptions into data structure that enables quick question answering and checks
"""

import gzip
import os
import re

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))


class Icd10Cm(object):

    def __init__(self):

        # code to description lookup dictionary
        self._cd2desc = {}

        # read in CMS data
        filename = os.path.join(CURRENT_PATH, 'icd10cm_codes_2020.txt.gz')
        with gzip.open(filename, 'rt') as file_handle:
            for line in file_handle:
                self._cd2desc[line[0:8].strip()] = line[8:].strip()

    def description(self, code: str) -> str:
        """Return the description for the given code, or black string if it does not exist"""
        return self._cd2desc.get(re.sub('\.', '', code.upper()), '')

    def code_match(self, lead_code: str) -> list:
        codes = []
        for cd in self._cd2desc:
            if re.match(lead_code, cd, re.IGNORECASE):
                codes.append(cd)
        return codes

    def search(self, text: str) -> list:
        """
        Return a list of ICD10 codes whose description contains the text, case insensitive.

        :param text:
        :return:
        """

        matching_codes = []

        regex = re.compile(text, re.IGNORECASE)
        for cd, desc in self._cd2desc.items():
            if regex.search(desc):
                matching_codes.append(cd)

        return matching_codes


if __name__ == '__main__':

    icd10 = Icd10Cm()
    #amyloid_codes = icd10.search('amyloid')
    #for cd in amyloid_codes:
    #    print(cd, icd10.description(cd))
    print(icd10.code_match('E85'))
    for cd in icd10.code_match('E85'):
        print(cd, icd10.description(cd))



