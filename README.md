# Equipo 9: Join + Secondary Sort en MapReduce

Ejemplo propio (tienda): une **clientes** con **pedidos** (join) y ordena
los pedidos de cada cliente por **fecha** (secondary sort). Hecho en Java con Hadoop 3.3.6.

Convención: 💻 = se ejecuta en tu computadora, 🐳 = se ejecuta dentro del contenedor.

## Requisitos
- Docker instalado
- El contenedor `hadoop` de la práctica (con Hadoop 3.3.6 en /opt/hadoop-3.3.6 y Java 17)

## Paso 1: Clonar el repositorio
💻
```
git clone <URL-DE-ESTE-REPO>
cd bda-equipo-9-retos
```

## Paso 2: Arrancar el contenedor (si está detenido)
💻
```
docker start hadoop
```

## Paso 3: Copiar los archivos al contenedor
💻
```
docker exec hadoop mkdir -p /root/tienda/datos
docker cp scripts/TiendaJoin.java hadoop:/root/tienda/
docker cp scripts/ejecutar.sh hadoop:/root/tienda/
docker cp datos/. hadoop:/root/tienda/datos/
```
Los archivos quedan en `/root/tienda` dentro del contenedor.

## Paso 4: Entrar al contenedor y ejecutar
💻
```
docker exec -it hadoop bash
```
🐳
```
cd /root/tienda
chmod +x ejecutar.sh
./ejecutar.sh
```

## Resultado esperado
Debe parecerse a `resultados/part-r-00000`: cada cliente con sus pedidos
en orden de fecha y una línea TOTAL (Ana 670, Luis 190, Marta 80).

## Cómo funciona
- **Map:** emite la llave (id_cliente, etiqueta). El cliente lleva "0" y cada pedido "1-fecha".
- **Partitioner:** reparte solo por id_cliente.
- **Comparador de agrupación:** un reduce() por cliente.
- **Reduce:** recibe primero al cliente (join) y luego sus pedidos ya ordenados (secondary sort).

## Estructura
- `datos/`: clientes.txt y pedidos.txt
- `scripts/`: TiendaJoin.java y ejecutar.sh
- `resultados/`: salida esperada
- `ejercicio/`: mini ejercicio y solución
