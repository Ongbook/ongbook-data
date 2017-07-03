from io import BytesIO
from collections import defaultdict
from heapq import nlargest

from luigi import six

import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.contrib.postgres

from lxml import etree


class InputFile(luigi.ExternalTask):
    
    def output(self):
        return luigi.LocalTarget('Entidades do Brasil.kml')


class ParseNGOSchemaFromXML(luigi.Task):

    def requires(self):
        return InputFile()

    def run(self):

        with self.input().open('r') as in_file:
            tree = etree.parse(in_file)
            root = tree.getroot()

            # for el in root.xpath('//kml:Placemark', namespaces=namespaces)[:1]:
            #     print(el.__dict__)
            selData = './ExtendedData/Data[@name="%s"]/value'
            for el in root.iter('Placemark'):
                ngo = {
                    'name': el.find('name').text,
                    'alternateName': el.find(selData % 'Nomeguerra').text, 
                    'foundingDate': el.find(selData % 'fundacao').text, 
                    'address': {
                        'streetAddress': el.find(selData % u'endereço').text,
                        'postalCode': el.find(selData % 'bairro').text,
                        'streetAddress': el.find(selData % 'cep').text,
                        'addressDistrict': el.find(selData % 'bairro').text,
                        'addressLocality': el.find(selData % 'cidade').text,
                        'addressRegion': el.find(selData % 'estado').text,
                        'addressCountry': 'BR',
                    }, 
                    'telephone': el.find(selData % 'telefone').text,
                    'fax': el.find(selData % 'fax').text,
                    'email': el.find(selData % 'e_mail').text,
                    'url': el.find(selData % 'link').text,
                    'financialProduct': [
                        {
                            'beneficiaryBank': el.find(selData % 'banco').text,
                            'agencyBank': el.find(selData % 'agencia').text,
                            'accountBank': el.find(selData % 'conta').text,
                            'currency': 'BRL',
                        }
                    ]
                }

                print(ngo)


if __name__ == "__main__":
    luigi.run()