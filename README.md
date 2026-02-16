# Crear README.md

readme_content= """ 
# Mi primer ETL con Python

## Pipeline ETL que procesa datos de e-commerce para generar emtrica de ventas

## Cómo correr
 ```bash
pip install pandad pyarrow
python etl.py

 ```

## Decisiones de limpieza
- **Nulos**: Eliminé filas sin customer_id, product_id (campos criticos)
- **Duplicados**: Eliminé duplicados por order_id, quedándome con el más reciente
- **Tipos**: Convertí order_date a datetime, quantity y total a numérico

## Output
-`ventas_cliente.csv`: Total gastado y cantidad de órdenes por cliente
-`ventas_mes.csv`: Ventas totales por mes
-`df_clean_orders.parquet`: Dataset limpio en formato actualizado

## Autor

Charly - 13/02/2026
"""

with open('README.md', 'w') as f 
    f.write(readme_content)

print("Archivo README.md creado")
