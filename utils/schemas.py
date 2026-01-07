import pandas as pd
import json

##################################################################################
# Data Chiper-Looker
##################################################################################

class BaseSchema:
    def __init__(self):
        self.df_schema = pd.DataFrame()
        self.map_dict = {}

    def to_json_list(self, df):
        # Genera lista de strings JSON, manejando fechas automáticamente
        json_str = df.to_json(orient='records', date_format='iso')
        json_list = json.loads(json_str)  # Lista de dicts
        return json_list


class MasterSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype='Int64'),
            "id_categoria": pd.Series(dtype='Int64'),
            "id_proveedor": pd.Series(dtype='Int64'),
            "fecha": pd.Series(dtype='datetime64[ns]'),
            "venta_neta": pd.Series(dtype='float64'),
            "ganancia_neta": pd.Series(dtype='float64'),
            "descuento_neto": pd.Series(dtype='float64'),
            "cantidad": pd.Series(dtype='Int64'),
            "iva": pd.Series(dtype='float64'),
            "front": pd.Series(dtype='float64'),
            "back": pd.Series(dtype='float64'),
            "nombre": pd.Series(dtype='object')
        })

        self.map_dict = {
            "sku": "Sales SKU",
            "nombre": "Sales Name SKU",
            "id_categoria": "Sales Marketplace Category ID",
            "id_proveedor": "Sales Provider ID",
            "fecha": "Sales Order Delivered At Date",
            "venta_neta": "Sales Product Sales At Dynamic",
            "ganancia_neta": "Sales Net Revenue",
            "descuento_neto": "Sales Total Discounted Value At Dynamic",
            "cantidad": "Sales Quantity",
            "iva": "Sales Last IVA (%)",
            "front": "Sales Product Front Gross Margin (%)",
            "back": "Sales Product Back Gross Margin (%)"
        }


class ProveedorSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "id": pd.Series(dtype='Int64'),
            "nombre": pd.Series(dtype='object'),
            "empresa": pd.Series(dtype='object')
        })
        self.map_dict = {
            "id": "Sales Provider ID",
            "nombre": "Sales Provider",
            "empresa": "Sales Provider Business Name"
        }

class MacroCategoriaSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "id": pd.Series(dtype='Int64'),
            "nombre": pd.Series(dtype='object')
        })
        self.map_dict = {
            "id": "Sales Marketplace Macro Category ID",
            "nombre": "Sales Marketplace Macro Category"
        }

class CategoriaSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "id": pd.Series(dtype='Int64'),
            "id_macro": pd.Series(dtype='Int64'),
            "nombre": pd.Series(dtype='object')
        })
        self.map_dict = {
            "id": "Sales Marketplace Category ID",
            "id_macro": "Sales Marketplace Macro Category ID",
            "nombre": "Sales Marketplace Category"
        }

class CompetidorSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype='Int64'),
            "fecha": pd.Series(dtype='datetime64[ns]'),
            "precio_lleno": pd.Series(dtype='float64'),
            "precio_descuento": pd.Series(dtype='float64'),
        })
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }

class SkuImgSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype='Int64'),
            "img": pd.Series(dtype='object')
        })
        self.map_dict = {
            "sku": "parentSku",
            "img": "parentImageURL"
        }

class MasterVentaUTTSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            'empresa': pd.Series(dtype='object'),
            'ano': pd.Series(dtype='int64'),
            'mes': pd.Series(dtype='int64'),
            'categoria': pd.Series(dtype='object'),
            'fabricante': pd.Series(dtype='object'),
            'marca': pd.Series(dtype='object'),
            'venta_resto': pd.Series(dtype='float64'),
            'cantidad': pd.Series(dtype='int64')
        })
        self.map_dict = {
            'empresa': 'empresa',
            'ano': 'agno',
            'mes': 'mes',
            'categoria': 'categoria',
            'fabricante': 'fabricante',
            'marca': 'marca',
            'venta_resto': 'VentaPesos',
            'cantidad': 'Ventauni'
        }

class FabricanteUTTSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            'nombre': pd.Series(dtype='object')
        })
        self.map_dict = {
            "nombre": "fabricante",
        }

class CategoriaUTTSchema(BaseSchema):
    def __init__(self):
        super().__init__()
        self.df_schema = pd.DataFrame({
            'nombre': pd.Series(dtype='object')
        })
        self.map_dict = {
            "nombre": "categoria"
        }