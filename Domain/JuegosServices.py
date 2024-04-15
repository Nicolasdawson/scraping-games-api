# from supabase import create_client, Client
# import httpx
# import logging
#
#
# url: str = "https://sdlkntguhdjuxfzbjvzt.supabase.co"
# key: str ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkbGtudGd1aGRqdXhmemJqdnp0Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcxMjk3MjQwMywiZXhwIjoyMDI4NTQ4NDAzfQ.y20s_kbJ3s0R3pLCG9NhC34RCjGhgWvqHYXaDggAV4U"
#
# supabase = create_client(url, key)
#
# async def getJuegos(filtro: dict):
#     page_size = filtro.get('take', 25)
#     offset = filtro.get('skip', 0)
#     plataforma = filtro.get('plataforma')
#
#     try:
#         query = supabase.table('juegos').select("*")
#
#         if plataforma is not None:
#             query = query.filter('plataformas', 'cs', f"{{{plataforma}}}")
#
#         response = query.limit(page_size).offset(offset).execute()
#
#         return response.data
#     except Exception as e:
#         return {"message": str(e)}
#
#
# async def postJuegosMasivo(filtro: dict):
#     page_size = filtro.get('take', 25)
#     offset = filtro.get('skip', 0)
#     plataforma = filtro.get('plataforma')
#     rawg_api_key = "d3888609ff954c03b422b603b4f6558d"
#
#     rawg_url = f"https://api.rawg.io/api/games?key={rawg_api_key}&platforms={plataforma}&page_size={page_size}&page={offset +1}&exclude_stores=true&exclude_game_series=true&exclude_parents=true&exclude_additions=true"
#
#     logging.info(rawg_url)
#
#     try:
#         async with httpx.AsyncClient() as client:
#             response = await client.get(rawg_url)
#             logging.info(response)
#             games_data = response.json()['results']
#             logging.info(f"Number of games retrieved: {len(games_data)}")  # Logging the count of games_data objects
#
#         successes, failures = 0, 0
#
#         for game in games_data:
#
#             game_insert = {
#                 "nombre": game["name"],
#                 "servicioexternoid": game["id"],
#                 "plataformas": '{' + plataforma + '}',
#                 "fechalanzamiento": game["released"],
#                 "rating": game["rating"],
#                 "imagen": game.get("background_image", ""),
#                 "servicioexterno": "rawg",
#                 "metacritic": game["metacritic"],
#             }
#
#             esrb_rating = game.get('esrb_rating')
#
#             if(esrb_rating is not None):
#                 game_insert['esrb'] = esrb_rating['name']
#             try:
#                 supabase.table('juegos').insert(game_insert).execute()
#                 successes += 1
#             except Exception as insert_error:
#                 failures += 1
#                 logging.error(f"Failed to insert game: {game_insert}, Error: {insert_error}")
#         return {"message": "Data processed", "successes": successes, "failures": failures}
#     except Exception as e:
#         logging.error(f"Failed to retrieve or process games: {e}")
#         return {"message": str(e)}


from supabase import create_client, Client
import httpx
import logging
import asyncio

url: str = "https://sdlkntguhdjuxfzbjvzt.supabase.co"
key: str ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkbGtudGd1aGRqdXhmemJqdnp0Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcxMjk3MjQwMywiZXhwIjoyMDI4NTQ4NDAzfQ.y20s_kbJ3s0R3pLCG9NhC34RCjGhgWvqHYXaDggAV4U"

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
    rawg_api_key = "d3888609ff954c03b422b603b4f6558d"

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