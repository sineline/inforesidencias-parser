from audioop import ratecv
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import json
from joblib import Parallel, delayed
import itertools


class inforesidencias:
    def __init__(self, region: str = "catalunya") -> None:

        self._BASE_URL = "https://www.inforesidencias.com"
        self._REQUEST_URL = self._BASE_URL + "/centros/buscador/residencias/"
        self.region = region
        self.totalPages = int()
        self.residencies = list()
        self.session = requests.Session()

        self.params = {
            "paginaActual": 1,
            "filtroBuscador.grupo": "",
            "filtroBuscador.textoLibre": "",
            "filtroBuscador.tipologia": 1,
            "filtroBuscador.comunidad": region,
            "filtroBuscador.provincia": "",
            "filtroBuscador.comarca": "",
            "filtroBuscador.poblacion": "",
            "filtroBuscador.precioMaximo": "",
            "filtroBuscador.genero": "",
            "filtroBuscador.ratioMinimoPersonalResidentes": "",
            "filtroBuscador.espacioMinimoPorResidente": "",
            "filtroBuscador.tipoEdificio": "",
            "filtroBuscador.ordenar": "valorTransparencia"
        }
        pass

    def get_residence_web(self, resid):
        req_url = self._BASE_URL + '/centros/datos-ajax/' + resid + '/web'
        return requests.get(req_url).text

    def get_residence_basic_data(self, html) -> dict:
        # basic data
        # nombre, direccion, geoloc, telf, web
        jsons = html.findAll('script', type='application/ld+json')
        residence_json = json.loads(jsons[0].text)

        res_keys_to_remove = ['@context', '@type',
                              'description', 'image', 'name']

        keys = list(residence_json.keys())
        for item in keys:
            if isinstance(residence_json.get(item), dict) or item in res_keys_to_remove:
                try:
                    del residence_json.get(item)['@type']
                except:
                    del residence_json[item]

        id_pattern = re.compile(r".*")
        link = html.findAll('a', {'data-id-centro': id_pattern})[0]
        internalId = link['data-id-centro']
        residence_json['infores_id'] = internalId
        residence_json['web_centre'] = self.get_residence_web(internalId)
        return residence_json

    def get_quality_data(self, html) -> dict:
        # quality data
        # ratio profs / 10 usuaris, transparencia
        data = dict()

        # ratio profs / 10 usuaris
        ratio = html.findAll('div', class_='residents-info')[0].find('span')
        data['ratio_profs_usuaris'] = float(ratio.text.replace(",", "."))

        # transparencia
        transp = html.find('div', class_='row values').findAll('span')[2].text
        data['pct_transparencia'] = float(transp.replace("%", ""))
        return data

    def get_facilities_data(self, html) -> dict:
        # places totals, m2 per usuari, tipo de residencia (jardin, etc)
        # piscina, cocina, zona verde, transport public, parking, unidad de demencia
        data = dict()

        space = html.find('div', id='card-facilities-space')
        data['plazas'] = int(space.findAll('p')[0].text)
        data['m2_usuari'] = float(space.findAll('p')[2].text.replace("m2", ""))
        data['m2_totales'] = data['plazas'] * data['m2_usuari']

        tipo_res = html.find('img', {'alt': 'tipo de residencia'}).parent

        data['tipo_residencia'] = tipo_res.text.strip()
        items = html.find('div', id='card-facilities-items').findAll('img')

        for item in items:
            item_name = item.text.strip()
            data[item_name] = (1, 0)[1 if item.find_parent('del') else 0]

        return data

    def get_financiacio_data(self, html) -> dict:
        # financiacio
        # preu habitacions desde, financiacio publica hombre, financiacio publica mujer, individual con baño,
        # individual sin baño, compartida hombre con baño, compartida mujer con baño, comartida hombre sin baño,
        # compartida mujer sin baño, Sistema precios
        data = dict()
        rooms = html.find('div', id='card-rooms')
        rooms_data = rooms.findAll('div', recursive=False)
        for item in rooms_data:
            cat = item.find('img')['alt']
            aval = not item.find('div', class_='h5').find('img')
            price = item.find(class_='h5').text.strip() if aval else None
            price = price.replace("€", "") if isinstance(price, str) else None
            data[cat] = float(price.replace('.', '')) if aval else None

        data['Precio_desde'] = min(i for i in data.values() if i is not None)

        finan = html.find(id='card-financing').findAll('i')
        data[finan[0].parent.text.strip()] = 'text-success' in finan[0]['class']
        data[finan[1].parent.text.strip()] = 'text-success' in finan[1]['class']

        sistema_precios = html.find(text='Sistemas de precios').find_next()
        data['sistema_precios'] = sistema_precios.text.strip()
        return data

    def get_admissions_data(self, html) -> dict:
        # admissions
        # admissions persona encamada, admissions persona silla ruedas, admission demencias
        data = dict()
        admissions = html.find(id='card-admissions').findAll('div')
        for item in admissions:
            cat = item.find('img')['alt']
            available = 'text-success' in item.find('i')['class']
            data[cat] = available

        return data

    def get_servicios_data(self, html) -> dict:
        # servicios
        # peluqueria, podologia, acompañamiento, rehabilitacion, eleccion menu, productos higiene, informes de salud
        data = dict()
        services = html.find(id='card-services').findAll('img')
        for item in services:
            alt = item['alt']
            key, value = alt.split(' - ')
            data[key] = value
        return data

    def get_professionales_data(self, html) -> dict:
        # professionales
        # Médico, Enfermera, Fisioterapeuta, Terapeuta ocupacional, Psicólogo,
        # Trabajador Social, Educador Social, Animador Sociocultural,
        # Logopeda, Farmacéutico, Otros profesionales

        data = dict()
        profs = html.find(text='Equipo de profesionales').find_next()
        profs_data = profs.findAll('li', recursive=False)

        for prof in profs_data:
            data[prof.text] = True

        return data

    def get_institucional_data(self, html) -> dict:
        # institucional
        # Titulo director, modelo contrato residente, reglamento de regimen interior,
        # horario de vida, organigrama, ultima inspeccion Serv. Socials, ultina inspeccion sanidad

        data = dict()
        titulo_dr = html.find(text='Titulación del/la director/a').find_next()
        data['titulacion director'] = titulo_dr.text.strip()

        docs = html.find(id='card-documentation').findAll('dt')
        fechas = html.find(id='card-documentation').findAll('dd')

        for item in zip(docs, fechas):
            key = item[0].text.strip()
            link = self._BASE_URL+item[0].find('a')['href']
            fecha = item[1].text.strip()
            data[key] = {'link': link, 'fecha': fecha}

        return data

    def get_certificaciones_data(self, html) -> dict:
        # certificaciones
        # Acreditado ley dependencia, certificado calidad,
        # politica de uso de contenciones, comite de etica, otras certificaciones

        data = dict()
        certs_list = html.find(text='Certificaciones').find_next()
        cert_name = certs_list.findAll('dt')
        cert_details = certs_list.findAll('dd')

        for cert in zip(cert_name, cert_details):
            key = cert[0].text.strip()
            value = cert[1].text.strip()
            data[key] = True if value == '' else value

        return data

    def get_residence_data(self, residence: dict) -> dict:

        residence_page = self.session.get(residence.get('url'))
        html = BeautifulSoup(residence_page.content, "html.parser")

        # basic data
        residence.update(self.get_residence_basic_data(html))

        # Quality data
        residence.update(self.get_quality_data(html))

        # Instalaciones
        residence.update(self.get_facilities_data(html))

        # financiacio
        residence.update(self.get_financiacio_data(html))

        # admissions
        residence.update(self.get_admissions_data(html))

        # serveis
        residence.update(self.get_servicios_data(html))

        # professionales
        residence.update(self.get_professionales_data(html))

        # datos institucion & documentacion
        residence.update(self.get_institucional_data(html))

        # certificaciones
        residence.update(self.get_certificaciones_data(html))

        return residence

    def get_paginated_page(self, page_number: int) -> BeautifulSoup:
        print(f"Page {page_number}")
        params = self.params.copy()
        params.update({"paginaActual": page_number})
        paginated_page = self.session.post(self._REQUEST_URL, data=self.params)
        html = BeautifulSoup(paginated_page.content, "html.parser")
        rawitems = html.find_all('div', class_='col-md-8')
        items = list()
        for item in rawitems:
            a = item.find_next('h2').find_next('a')
            items.append({'name': a.text, 'url': self._BASE_URL + a['href']})

        data = Parallel(n_jobs=10)(delayed(self.get_residence_data)(page)
                                   for page in items)
        return data

    def get_residencies(self) -> list:

        try:
            firstPage = self.session.post(self._REQUEST_URL, data=self.params)
        except:
            return {'status_code': firstPage.status_code, 'message': firstPage.error_message}
        resultregex = r"(\d+(?=\sresultados))"
        parsedPages = re.findall(resultregex, firstPage.text)[0]
        self.totalPages = 1 + (int(parsedPages) // 10)

        print(f"Total pages: {self.totalPages}")

        residencies = Parallel(n_jobs=5)(delayed(self.get_paginated_page)(
            page) for page in range(1, self.totalPages))

        self.residencies = list(itertools.chain.from_iterable(residencies))
        return self.residencies
