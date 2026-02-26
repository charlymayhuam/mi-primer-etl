import pandas as pd
import glob
import os

# Verificando archivos CSV de E-commerce

archivos= glob.glob('data/ecommerce_*.csv')

if not archivos:
    print("Archivos no encontrados, asegurese en buscar bien en la ruta data/ y ubicar la carpeta.")
    print("Los archivos esperados son /ecommerce_brands.csv /ecommerce_categories.csv /ecommerce_customers.csv /ecommerce_inventory.csv /ecommerce_order_items.csv /ecommerce_orders.csv /ecommerce_products.csv /ecommerce_promotions.csvecommerce_reviews.csv /ecommerce_suppliers.csv /ecommerce_warehouses.csv")
else:
    print(f"Archivos encontrados: {len(archivos)}")
    for f in sorted(archivos):
        print(f" - {os.path.basename(f)}")

# Cargando los archivos CSV principales

df_orders=pd.read_csv('data/ecommerce_orders.csv')
df_orders_items=pd.read_csv('data/ecommerce_order_items.csv')
df_products=pd.read_csv('data/ecommerce_products.csv')
df_customers=pd.read_csv('data/ecommerce_customers.csv')

# Explorando 

print(f"\n Resumen: ")
print(f"Orders: {len(df_orders)} filas, {len(df_orders.columns)} columnas ")
print(f"Products: {len(df_products)} filas, {len(df_products.columns)} columnas ")
print(f"Order Items: {len(df_orders_items)} filas, {len(df_orders_items.columns)} columnas ")
print(f"Customers: {len(df_customers)} filas, {len(df_customers.columns)} columnas ")

print("\n Primeras filas de ordenes:")
print(df_orders.head())

print("\n Información de ordenes:")
print(df_orders.info())

# Transformando datos

print(f"\n Contando nulos: ")
print(df_orders.isnull().sum())

# Decidir con la limpieza

# Si son pocos (<5%): Eliminar las filas
# Si son muchos: Rellenar con valores iguales a 0

# Eliminamos filas con campos criticos
df_clean_orders = df_orders.dropna(subset=['order_id','customer_id','subtotal'])

# Reemplazamos valores con 0 en campos numericos opcionales
df_clean_orders['discount_percent']= df_clean_orders['discount_percent'].fillna(0)

print(f"Filas antes: {len(df_orders)} . Filas después: {len(df_clean_orders)}")

# Eliminamos duplicados 

print(f"\n Contando duplicados: ")
print(df_orders.duplicated().sum())

df_clean_orders = df_orders.sort_values('order_date').drop_duplicates(subset=['customer_id','order_number'], keep='last')


print(f"Filas antes: {len(df_orders)} . Filas sin duplicar: {len(df_clean_orders)}")

# Vemos tipos de datos actuales

print("\n Tipos de datos actuales: ")
print(df_clean_orders.dtypes)

# Convertimos la tabla order_date a formato fecha

df_clean_orders['order_date']= pd.to_datetime(df_clean_orders['order_date'])

# Aseguramos que los números sean numéricos

df_clean_orders['subtotal'] = pd.to_numeric(df_clean_orders['subtotal'], errors='coerce')
df_clean_orders['shipping_cost'] = pd.to_numeric(df_clean_orders['shipping_cost'], errors='coerce')

# Verificamos

print("\n Tipos después de conversión: ")
print(df_clean_orders.dtypes)

# PREGUNTA 1:  5 CLIENTES QUE MAS GASTARON
# Agrupamos por customer_id y sumamos el total_amount

ventas_clientes= df_clean_orders.groupby('customer_id').agg({
    'total_amount' : 'sum',
    'order_id' : 'count'
}).rename(columns={'total_amount':'total_gastado', 'order_id':'cantidad_ordenes'} )

ventas_clientes=ventas_clientes.sort_values('total_gastado', ascending=False)

print("\n Top 5 Clientes:")
print(ventas_clientes.head())

# PREGUNTA 2: PRODUCTOS MAS VENDIDOS
# Agrupamos por order_id y sumamos el quantity

productos_vendidos=df_orders_items.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
print(f"\n Producto más vendido: ID {productos_vendidos.idxmax()} . Vendio {productos_vendidos.max()} unidades ")

# PREGUNTA 3: EVOLUCIÓN MENSUAL POR VENTAS
# Agrupamos por order_date y sumamos total_amount

df_clean_orders['mes']=df_clean_orders['order_date'].dt.to_period('M') 
ventas_mes= df_clean_orders.groupby('mes')['total_amount'].sum().reset_index()
ventas_mes.columns=['mes','total_ventas']

print(f"\nVentas de cada mes:")
print(ventas_mes)

# AHORA HACEMOS EL LOAD
# Creando el archivo output si no existe 

import os
os.makedirs('output',exist_ok=True)

# Guardamos las métricas en CSV

ventas_clientes.to_csv('output/ventas_por_cliente.csv', index=False)
productos_vendidos.to_csv('output/productos_mas_vendidos.csv', index=False)
ventas_mes.to_csv('output/ventas_mes.csv', index=False)

# Guardamos los datos limpios en CSV 

df_clean_orders.to_csv('output/clean_orders.csv', index=False)

print("\nArchivos guardados en OUTPUT")

# Guardamos las métricas en Parquet

df_clean_orders.to_parquet('output/clean_orders.parquet', index=False)

# Calculando tamaño CSV vs Parquet

csv_size= os.path.getsize('output/clean_orders.csv') / 1024
parquet_size= os.path.getsize('output/clean_orders.parquet') / 1024

print(f"Tamaño de archivo limpio csv: {csv_size:.1f} KB")
print(f"Tamaño de archivo limpio Parquet: {parquet_size:.1f} KB")