import parse_classes as pc


pr = pc.inforesidencias()
# resis = pr.get_residencies()

singleRes = pr.get_residence_data({'name':'testres', 
                                   'url':'https://www.inforesidencias.com/centros/residencia/2322/centres-geriatrics-malgrat'})
print(singleRes)
