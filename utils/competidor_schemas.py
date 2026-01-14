import pandas as pd
import json

##################################################################################
# Schemas SOLO para competidores (misma estructura BaseSchema)
##################################################################################

class BaseSchema:
    def __init__(self):
        self.df_schema = pd.DataFrame()
        self.map_dict = {}

    def to_json_list(self, df):
        # Genera lista de dicts, manejando fechas automáticamente
        json_str = df.to_json(orient="records", date_format="iso")
        json_list = json.loads(json_str)
        return json_list


class GenericCompetidorSchema(BaseSchema):
    """
    Schema genérico (fallback) si no hay uno específico para el competidor.
    Ajusta map_dict si un competidor nuevo trae nombres distintos.
    """
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype="Int64"),
            "fecha": pd.Series(dtype="datetime64[ns]"),
            "precio_lleno": pd.Series(dtype="float64"),
            "precio_descuento": pd.Series(dtype="float64"),
        })
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }


class CentralMayoristaSchema(GenericCompetidorSchema):
    """
    Central Mayorista.
    Si el BQ view cambia nombres, modifica SOLO este map_dict.
    """
    def __init__(self):
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }


class AlviSchema(GenericCompetidorSchema):
    """
    Alvi (placeholder robusto).
    Si su view usa otras columnas, ajusta map_dict aquí.
    """
    def __init__(self):
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_min_price",
        }


class AdelcoSchema(GenericCompetidorSchema):
    """
    Adelco (placeholder robusto).
    """
    def __init__(self):
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }


class LaOfertaSchema(GenericCompetidorSchema):
    """
    La Oferta (placeholder robusto).
    """
    def __init__(self):
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }
