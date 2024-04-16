from supabase import create_client
import httpx
import logging
import asyncio
from dotenv import load_dotenv
import os
import requests
from bs4 import BeautifulSoup

load_dotenv()

url: str = os.getenv("URL_SUPABASE")
key: str = os.getenv("API_KEY_SUPABASE")

supabase = create_client(url, key)

async def getJuegos(filtro: dict):
    page_size = filtro.get('take', 25)
    offset = filtro.get('skip', 0)
    plataforma = filtro.get('plataforma')

    try:
        query = supabase.table('juegos').select("*")

        if plataforma is not None:
            query = query.filter('plataformas', 'cs', f"{{{plataforma}}}")

        response = query.limit(page_size).offset(offset).execute()

        return response.data
    except Exception as e:
        return {"message": str(e)}


async def postJuegosMasivo(filtro: dict):
    page_size = filtro.get('take', 25)
    offset = filtro.get('skip', 0)
    plataforma = filtro.get('plataforma')
    rawg_api_key = os.getenv("API_KEY_RAWG")

    keep_fetching = True

    while keep_fetching:
        rawg_url = f"https://api.rawg.io/api/games?key={rawg_api_key}&platforms={plataforma}&page_size={page_size}&page={offset + 1}&exclude_stores=true&exclude_game_series=true&exclude_parents=true&exclude_additions=true"

        logging.info(rawg_url)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(rawg_url)
                if response.status_code == 502:
                    logging.error("Received 502 from RAWG API, retrying in 5 minutes...")
                    await asyncio.sleep(300)
                    continue
                elif response.status_code != 200:
                    logging.error(f"Failed to fetch data from RAWG API: Status code {response.status_code}")
                    logging.error(f"Offset: {offset}")
                    logging.error(f"Url: {rawg_url}")
                    break

                games_data = response.json().get('results', [])
                if not games_data:
                    logging.info("No more games data available.")
                    logging.error(f"Offset: {offset}")
                    logging.error(f"Url: {rawg_url}")
                    break

                logging.info(f"Processing page {offset + 1} with {len(games_data)} games.")

            successes, failures = 0, 0

            for game in games_data:
                game_insert = {
                    "nombre": game["name"],
                    "servicioexternoid": game["id"],
                    "plataformas": '{' + plataforma + '}',
                    "fechalanzamiento": game["released"],
                    "rating": game["rating"],
                    "imagen": game.get("background_image", ""),
                    "servicioexterno": "rawg",
                    "metacritic": game.get("metacritic", 0),
                }

                esrb_rating = game.get('esrb_rating')

                if (esrb_rating is not None):
                    game_insert['esrb'] = esrb_rating['name']

                try:
                    supabase.table('juegos').insert(game_insert).execute()
                    successes += 1
                except Exception as insert_error:
                    failures += 1
                    logging.error(f"Failed to insert game: {game_insert}, Error: {insert_error}")

            logging.info(f"Page {offset + 1} processed: {successes} successes, {failures} failures")
        except Exception as e:
            logging.error(f"Failed to retrieve or process games: {e}")
            logging.error(f"Offset: {offset}")
            logging.error(f"Url: {rawg_url}")
            break

        await asyncio.sleep(120)
        offset += 1

    return {"message": "Data fetching and processing completed"}

# async def postTiendaJuegosService(tiendaId: str):
#
#     url = 'https://www.todojuegos.cl/Productos/PS5/_Juegos/Default.asp?ListMaxShow=50&ListOrderBy=&offset='
#     response = requests.get(url)
#
#     response.encoding = 'windows-1252'
#
#     soup = BeautifulSoup(response.text, 'html.parser')
#
#     games_data = []
#     for tr in soup.find_all('tr'):
#
#         linkObj = tr.find('a', style='font-size:18px; font-weight:bold;')
#         if linkObj:
#             if(linkObj['href'].startswith('//www.todojuegos.cl/Productos/PS5')):
#                 link = 'https:' + linkObj['href']
#                 title = linkObj.text
#
#                 price_tag = tr.find('p', style='color:#FF0000; font-size:20px; font-weight:bolder;')
#                 if price_tag:
#                     price = price_tag.text.strip()
#
#                     games_data.append({
#                         'title': title,
#                         'link': link,
#                         'price': price
#                     })
#
#     for game in games_data:
#         print(f"Title: {game['title']}, Link: {game['link']}, Price: {game['price']}")
#
#     ## ir a buscar la tienda a bd
#
#     ## ir a buscar la configuracion a la bd
#
#     ## ir a buscar el link de la tienda
#
#     ## empezar a iterar por plataforma
#
#     ## comparar el nombre del titulo con alguno que tenga en bd
#
#     return {"message": "Data fetching and processing completed"}