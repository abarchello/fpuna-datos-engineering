# Documentación de Entregables

Esta carpeta contiene los documentos cortos que acompañan las entregas de las
tareas individuales de la materia **Introducción a la Ingeniería de Datos**
(MIAAD FPUNA 2026). Cada documento incluye una descripción de los entregables
de la tarea correspondiente y la captura del DAG generado por dbt docs.

## Contenido

| Archivo | Tarea | Contenido |
|---|---|---|
| `Tarea_5_Capturas.pdf` | Tarea 5 | Proyecto dbt con 3 capas, listado de modelos staging / intermediate / marts, sources configurados, captura del DAG |
| `Tarea_6_Capturas.pdf` | Tarea 6 | Tests genéricos, tests de dbt-expectations, singular tests, documentación de modelos, captura del DAG con documentación, resultado de `dbt build` |

## Resultado de la ejecución completa

La última ejecución de `dbt build` sobre todo el proyecto produjo:

```
Done. PASS=83  WARN=8  ERROR=0  SKIP=0  NO-OP=0  TOTAL=91
```

83 tests pasaron, 8 warnings documentados (outliers reales de sensores y
duplicados del modo Append de Airbyte para el conector PokeAPI), cero errores
de ejecución sobre datos reales (~1.6 millones de registros de eCommerce y
~270 mil mediciones hidrométricas).
