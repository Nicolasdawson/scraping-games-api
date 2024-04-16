import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client
import logging

load_dotenv()

url: str = os.getenv("URL_SUPABASE")
key: str = os.getenv("API_KEY_SUPABASE")

supabase = create_client(url, key)
async def postJuegoPrecio():
    plataformas = ['PS4', 'PS5', 'Nintendo_Switch', 'XONE']
    tiendaId = 1

    for plataforma in plataformas:
        successes, failures = 0, 0
        initial_offset = 50
        last_html = None

        for offset in range(initial_offset, initial_offset + 50):
            url = f'https://www.todojuegos.cl/Productos/{plataforma}/_Juegos/Default.asp?ListMaxShow=50&ListOrderBy=&offset={offset}'

            response = requests.get(url)
            response.encoding = 'windows-1252'
            current_html = response.text

            if current_html == last_html:
                logging.info(f"No new data found for platform {plataforma} at offset {offset}. Stopping fetch.")
                break

            last_html = current_html

            soup = BeautifulSoup(current_html, 'html.parser')

            for tr in soup.find_all('tr'):
                linkObj = tr.find('a', style='font-size:18px; font-weight:bold;')
                if linkObj:
                    if (linkObj['href'].startswith(f'//www.todojuegos.cl/Productos/{plataforma}')):
                        link = 'https:' + linkObj['href']
                        title = linkObj.text

                        price_tag = tr.find('p', style='color:#FF0000; font-size:20px; font-weight:bolder;')
                        if price_tag:
                            price = price_tag.text.strip()

                            responseQuery = supabase.rpc('fuzzy_game_search', {'title': title}).execute()

                            if responseQuery.data:
                                juegoId = responseQuery.data[0]['id']

                                if (juegoId is not None):
                                    games_data = {
                                        'precio': price,
                                        'juegoId': juegoId,
                                        'tiendaId': tiendaId,
                                        'url': link
                                    }

                                    supabase.table('juegoPrecio').insert(games_data).execute()
                                    successes += 1
                                else:
                                    failures += 1
                                    logging.error(f"Juego con titulo {title} no encontró Id")
                            else:
                                failures += 1
                                logging.error(f"Juego con titulo {title} no encontrado en la BD")

    logging.info(f"Plataforma {plataforma} successes: {successes}")
    logging.info(f"Plataforma {plataforma} failures: {failures}")
    return {"message": "Data fetching and processing completed"}