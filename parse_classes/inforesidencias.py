import pandas
import requests
import re
from bs4 import BeautifulSoup
import os
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

    def get_residency_data(self, residency_url: str) -> dict:
        return {}

    def get_paginated_page(self, page_number: int) -> BeautifulSoup:
        print(f"Page {page_number}")
        params = self.params.copy()
        params.update({"paginaActual": page_number})
        paginated_page = self.session.post(self._REQUEST_URL, data=self.params)
        htmlcontent = BeautifulSoup(paginated_page.content, "html.parser")
        rawitems = htmlcontent.find_all('div', class_='col-md-8')
        items = list()
        for item in rawitems:
            a = item.find_next('h2').find_next('a')
            items.append({'name': a.text, 'url': self._BASE_URL + a['href']})

        data = Parallel(n_jobs=10)(delayed(self.get_residency_data)(page)
                                   for page in range(1, len(items)))
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
