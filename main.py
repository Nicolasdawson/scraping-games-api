from fastapi import FastAPI, HTTPException
app = FastAPI()
from Domain.JuegosServices import getJuegos as getJuegosService, postJuegosMasivo, postTiendaJuegosService
from typing import Optional
import logging

logging.basicConfig(level=logging.DEBUG)

@app.get("/tiendas/{tiendaId}")
async def postTiendaJuegos(tiendaId):
    try:
        return await postTiendaJuegosService(tiendaId)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/juegos")
async def getJuegos(plataforma: Optional[str],
                    take: Optional[int],
                    skip: Optional[str]):
    filtro = {
        "plataforma": plataforma,
        "take": take,
        "skip": skip
    }

    try:
        return await getJuegosService(filtro)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/juegos")
async def postJuegos(plataforma: Optional[str] = None,
                    take: Optional[int] = None,
                    skip: Optional[int] = None):
    filtro = {
        "plataforma": plataforma,
        "take": take,
        "skip": skip
    }

    try:
        return await postJuegosMasivo(filtro)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

