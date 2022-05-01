import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import json
from joblib import Parallel, delayed
import itertools


class inforesidencias:
    def __init__(self, region: str = "catalunya", provincia: str = '', comarca: str = '', output='tabulated', filename=None) -> None:
        """ Clase para obtener datos de residencia de inforesidencias.es

        Args:
            region (str, optional): Region de busqueda. Defaults to "catalunya".
            provincia (str, optional): Provincia de busqueda. Defaults to ''.
            comarca (str, optional): Comarca de busqueda. Defaults to ''.
            output (str: optional) {'normalized', 'tabulated', 'raw'}: 
                    Defaults to 'tabulated'. 
                    If 'normalized', returns a flattened Dataframe. 
                    If 'tabulated', returns a tabulated Dataframe. 
                    If 'raw', returns a raw dict.
            filename (str, optional): Nombre del fichero de salida. Defaults to None. If None, returns a Dataframe.
        """
        if output not in ['normalized', 'tabulated', 'raw']:
            raise ValueError('output must be one of "normalized", "tabulated", "raw"')
        
        if type(filename) not in [str, None]:
            raise ValueError('output must be None or a string')
        
        self._BASE_URL = "https://www.inforesidencias.com"
        self._REQUEST_URL = self._BASE_URL + "/centros/buscador/residencias/"
        self.region = region
        self.totalPages = int()
        self.residencies = list()
        self.session = requests.Session()
        self.output = output
        self.filename = filename
        self.params = {
            "paginaActual": 1,
            "filtroBuscador.grupo": "",
            "filtroBuscador.textoLibre": "",
            "filtroBuscador.tipologia": 1,
            "filtroBuscador.comunidad": region,
            "filtroBuscador.provincia": provincia,
            "filtroBuscador.comarca": comarca,
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
        """ Obtiene la web de la residencia
        Args:
            resid (_type_): id interna de la residencia

        Returns:
            _type_: web de la residencia
        """
        req_url = self._BASE_URL + '/centros/datos-ajax/' + resid + '/web'
        return requests.get(req_url).text

    def get_residence_basic_data(self, html) -> dict:
        """ Obtiene los datos básicos de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: diccionario con los datos básicos de la residencia
        """
        # basic data
        # nombre, direccion, geoloc, telf, web
        jsons = html.findAll('script', type='application/ld+json')
        residence_json = json.loads(jsons[0].text)

        res_keys_to_remove = ['@context', '@type', 'url',
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
        """ Obtiene los datos de calidad de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de calidad de la residencia
        """
        # quality data
        # ratio profs / 10 usuaris, transparencia
        data = dict()

        # ratio profs / 10 usuaris
        ratio = html.findAll('div', class_='residents-info')[0].find('span')
        try:
            data['ratio_profs_usuaris'] = float(ratio.text.replace(",", "."))
        except:
            data['ratio_profs_usuaris'] = "None"

        # transparencia
        transp = html.find('div', class_='row values').findAll('span')[-1].text
        data['pct_transparencia'] = float(transp.replace("%", ""))
        return data

    def get_facilities_data(self, html) -> dict:
        """ Obtiene los datos de las instalaciones de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de las instalaciones de la residencia
        """
        # places totals, m2 per usuari, tipo de residencia (jardin, etc)
        # piscina, cocina, zona verde, transport public, parking, unidad de demencia
        data = dict()

        space = html.find('div', id='card-facilities-space')
        rawspacetext = space.text.strip().replace('\n', ' ')

        try:
            data['plazas'] = re.findall(r'\d+(?!plazas)', rawspacetext)[0]
        except:
            data['plazas'] = None

        try:
            data['m2_usuari'] = re.findall(r'\d+(?=m2)', rawspacetext)[0]
        except:
            data['m2_usuari'] = None

        try:
            data['m2_totales'] = data['plazas'] * data['m2_usuari']
        except:
            data['m2_totales'] = None

        tipo_res = html.find('img', {'alt': 'tipo de residencia'}).parent

        data['tipo_residencia'] = tipo_res.text.strip()
        items = html.find('div', id='card-facilities-items').findAll('img')

        for item in items:
            item_name = item.text.strip()
            data[item_name] = (1, 0)[1 if item.find_parent('del') else 0]

        return data

    def get_financiacio_data(self, html) -> dict:
        """ Obtiene los datos de financiación de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de financiación de la residencia
        """
        # financiacio
        # preu habitacions desde, financiacio publica hombre, financiacio publica mujer, individual con baño,
        # individual sin baño, compartida hombre con baño, compartida mujer con baño, comartida hombre sin baño,
        # compartida mujer sin baño, Sistema precios
        data = dict()
        rooms = html.find('div', id='card-rooms')
        rooms_data = rooms.findAll('div', recursive=False)
        for item in rooms_data:
            cat = item.find('img')['alt']
            aval = bool(re.match(r'\d+[,.]?\d+', item.text))
            price = item.find(class_='h5').text.strip() if aval else None
            price = price.replace("€", "") if isinstance(price, str) else None
            data[cat] = float(price.replace('.', '')) if aval else None

        data['Precio_desde'] = min(set(data.values())) if data else None

        finan = html.find(id='card-financing').findAll('i')
        data[finan[0].parent.text.strip()] = 'text-success' in finan[0]['class']
        data[finan[1].parent.text.strip()] = 'text-success' in finan[1]['class']
        try:
            sistema_precios = html.find(text='Sistemas de precios').find_next()
            data['sistema_precios'] = sistema_precios.text.strip()
        except:
            data['sistema_precios'] = None

        return data

    def get_admissions_data(self, html) -> dict:
        """ Obtiene los datos de admisión de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de admisión de la residencia
        """
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
        """ Obtiene los datos de servicios de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de servicios de la residencia
        """
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
        """ Obtiene los datos de los profesionales de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de los profesionales de la residencia
        """
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
        """ Obtiene los datos de la institucional de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de la institucional de la residencia
        """
        # institucional
        # Titulo director, modelo contrato residente, reglamento de regimen interior,
        # horario de vida, organigrama, ultima inspeccion Serv. Socials, ultina inspeccion sanidad

        data = dict()
        try:
            titulo_dr = html.find(
                text='Titulación del/la director/a').find_next()
            data['titulacion director'] = titulo_dr.text.strip()
        except:
            data['titulacion director'] = 'No data'

        try:
            docs = html.find(id='card-documentation').findAll('dt')
            fechas = html.find(id='card-documentation').findAll('dd')

            for item in zip(docs, fechas):
                key = item[0].text.strip()
                link = self._BASE_URL+item[0].find('a')['href']
                fecha = item[1].text.strip()
                data[key] = {'link': link, 'fecha': fecha}
        except:
            data["Modelo de contrato de residente"] = 'No data'
            data["Reglamento de régimen interior"] = 'No data'
            data["Horario de vida"] = 'No data'
            data["Organigrama"] = 'No data'
            data["Última inspección Servicios Sociales"] = 'No data'
            data["Última inspección Sanidad"] = 'No data'

            pass

        return data

    def get_certificaciones_data(self, html) -> dict:
        """ Obtiene los datos de las certificaciones de la residencia

        Args:
            html (_type_): html de la pagina de la residencia

        Returns:
            dict: dict con los datos de las certificaciones de la residencia
        """
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
        """ Obtiene los datos de la residencia

        Args:
            residence (dict): dict con el nombre y la url de la residencia en inforesidencias.com

        Returns:
            dict: dict con los datos parseados de la residencia
        """
        residence_page = self.session.get(residence.get('url'))
        html = BeautifulSoup(residence_page.content, "html.parser")

        # basic data
        try:
            residence['dades_basiques'] = self.get_residence_basic_data(html)
        except Exception as e:
            print("Basic data: {}".format(e))
            residence['dades_basiques'] = {}

        # Quality data
        try:
            residence['qualitat'] = self.get_quality_data(html)
        except Exception as e:
            print("Quality data: {}".format(e))
            residence['qualitat'] = {}
            # {'ratio_profs_usuaris': "No data", 'pct_transparencia': 'No data'},

        # Instalaciones
        try:
            residence['instalacions'] = self.get_facilities_data(html)
        except Exception as e:
            print("Facilities data: {}".format(e))
            residence['instalacions'] = {}

        # financiacio
        try:
            residence['financiacio'] = self.get_financiacio_data(html)
        except Exception as e:
            print("Financiacio data: {}".format(e))
            residence['financiacio'] = {}

        # admissions
        try:
            residence['admissions'] = self.get_admissions_data(html)
        except Exception as e:
            print("Admissions data: {}".format(e))
            residence['admissions'] = {}

        # serveis
        try:
            residence['serveis'] = self.get_servicios_data(html)
        except Exception as e:
            print("Services data: {}".format(e))
            residence['serveis'] = {}

        # professionales
        try:
            residence['professionals'] = self.get_professionales_data(html)
        except Exception as e:
            print("Professionales data: {}".format(e))
            residence['professionals'] = {}

        # datos institucion & documentacion
        try:
            residence['institucional'] = self.get_institucional_data(html)
        except Exception as e:
            print("Institucional data: {}".format(e))
            residence['institucional'] = {}

        # certificaciones
        try:
            residence['certificacions'] = self.get_certificaciones_data(html)
        except Exception as e:
            print("Certificacions data: {}".format(e))
            residence['certificacions'] = {}

        return residence

    def get_paginated_page(self, page_number: int) -> BeautifulSoup:
        """ Obtiene las urls de las residencias de la pagina indicada

        Args:
            page_number (int): numero de la pagina de la busqueda residencias

        Returns:
            BeautifulSoup: html de la pagina de la busqueda residencias
        """
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

    def get_residencies(self) -> pd.DataFrame:
        """ Obtiene los datos de la busqueda de residencias

        Returns:
            DataFrame: DataFrame con los datos de la busqueda de residencias
        """
        try:
            firstPage = self.session.post(self._REQUEST_URL, data=self.params)
        except:
            return {'status_code': firstPage.status_code, 'message': firstPage.error_message}

        resultregex = r"(\d+(?=\sresultados))"
        parsedPages = re.findall(resultregex, firstPage.text)[0]
        self.totalPages = 1 + (int(parsedPages) // 10)

        print(f"Total pages: {self.totalPages}")

        residencies = Parallel(n_jobs=20)(delayed(self.get_paginated_page)(
            page) for page in range(1, self.totalPages))

        joined_residencies = list(itertools.chain.from_iterable(residencies))
        if self.output == 'normalized':
            normalized_residencies = pd.json_normalize(
                joined_residencies).set_index(['name'])
            normalized_residencies.columns = normalized_residencies.columns.str.split(
                ".", expand=True)
            self.residencies = normalized_residencies
        elif self.output == 'tabulated':
            residencies = pd.json_normalize(
                joined_residencies).set_index('name').stack()
            self.residencies = residencies.to_frame().reset_index().set_index('name')
            self.residencies.columns = ['datapoint', 'value']
        elif self.output == 'raw':
            self.residencies = joined_residencies
            
        if self.filename is not None:
            filename = f"{self.filename}.csv"
            self.residencies.to_csv(filename, encoding='utf-8-sig')

        return self.residencies
