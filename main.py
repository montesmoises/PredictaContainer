from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime, timedelta


app = FastAPI()

# Modelos Pydantic actualizados
class ContainerPart(BaseModel):
    part_number: str
    quantity: int

class ContainerData(BaseModel):
    container_id: str
    availability_date: datetime
    parts: List[ContainerPart]

    @field_validator("container_id", mode="before")
    def convert_to_string(cls, value):
        # Convierte cualquier tipo a string (ej: 12345 → "12345")
        return str(value)

class DataModel(BaseModel):
    forecast_data: List[Dict[str, Any]]
    stock_data: List[Dict[str, Any]]
    # containers_data: List[Dict[str, Any]]
    containers_data: List[ContainerData]

def calcular_metricas(contenedor: dict, stock_actual: dict) -> float:
    """Calcula la métrica de prioridad para un contenedor"""
    score = 0
    for part in contenedor['parts']:
        part_number = part['part_number'].strip()
        cantidad_contenedor = part['quantity']

        # Obtener déficit actual del stock
        deficit = -stock_actual.get(part_number, 0)

        if deficit > 0:
            # Calcular cobertura efectiva (mínimo entre lo que trae y lo que falta)
            cobertura = min(cantidad_contenedor, deficit)
            # Priorizar cobertura exacta (evitar exceso)
            score += cobertura * 100 - abs(cantidad_contenedor - deficit)

    return score

@app.post("/procesar_json/")
def procesar_json(data: DataModel):
    # Convertir datos a DataFrames
    df_forecast = pd.DataFrame(data.forecast_data)
    df_stock = pd.DataFrame(data.stock_data)
    df_containers = pd.DataFrame([c.model_dump() for c in data.containers_data])

    # Preparar datos
    df_forecast['required_date'] = pd.to_datetime(df_forecast['required_date'])
    df_forecast = df_forecast.sort_values('required_date')
    # Convertir a string ambos DataFrames (por si hay números u otros tipos)
    df_stock["part_number"] = df_stock["part_number"].astype(str)
    df_forecast["part_number"] = df_forecast["part_number"].astype(str).str.strip()

    stock = df_stock.set_index('part_number')['quantity'].to_dict()
    resultados = []



    partes_validos = df_stock['part_number'].unique()
    df_forecast = df_forecast[df_forecast['part_number'].isin(partes_validos)]
    contenedores_restantes = df_containers.to_dict('records')

    # for cantidad in df_forecast['required_quantity']:
    #     print(cantidad)

    for fecha in df_forecast['required_date'].unique():
        fecha_actual = pd.to_datetime(fecha)
        fecha_limite = fecha_actual + timedelta(weeks=2)

        # Filtrar contenedores disponibles
        contenedores_disponibles = [
            c for c in contenedores_restantes
            if fecha_actual <= pd.to_datetime(c['availability_date']) <= fecha_limite
        ]

        # Actualizar stock con requerimientos del forecast
        for _, row in df_forecast[df_forecast['required_date'] == fecha].iterrows():
            part_number = row['part_number']
            stock[part_number] = stock.get(part_number, 0) - row['required_quantity']

        contenedores_seleccionados = []
        for _ in range(4):
            # Actualizar la lista de disponibles CON los restantes
            contenedores_disponibles = [
                c for c in contenedores_restantes
                if fecha_actual <= pd.to_datetime(c['availability_date']) <= fecha_limite
            ]

            if not contenedores_disponibles:
                break

            # Calcular métricas
            metricas = []
            for contenedor in contenedores_disponibles:
                score = calcular_metricas(contenedor, stock)
                metricas.append((contenedor, score))

            # Seleccionar mejor contenedor
            mejor_contenedor, _ = max(metricas, key=lambda x: x[1])

            # Actualizar stock
            for part in mejor_contenedor['parts']:
                part_number = part['part_number'].strip()
                stock[part_number] = stock.get(part_number, 0) + part['quantity']

            # Registrar y eliminar
            contenedores_seleccionados.append(mejor_contenedor['container_id'])
            contenedores_restantes = [c for c in contenedores_restantes
                                    if c['container_id'] != mejor_contenedor['container_id']]

        resultados.append({
            "fecha": fecha_actual.date(),
            "contenedores_seleccionados": contenedores_seleccionados,
            # "stock_actualizado": stock.copy()
        })
    # print(contenedores_restantes)
    for c in contenedores_restantes:
        print(c)
    return {
        "resultados": resultados,
        "stock_final": stock,
        "contenedores_restantes": [c['container_id'] for c in contenedores_restantes]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="10.1.51.200", port=8000)