from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["deportes"]


def ejecutar_caso_de_prueba(nombre_prueba, procedimiento, resultado_esperado):
    print(f"Ejecutando prueba: {nombre_prueba}")
    try:
        resultado_obtenido = procedimiento()
        if resultado_obtenido == resultado_esperado:
            print(f"Resultado: APROBADO\n")
            return True
        else:
            print(f"Resultado: FALLIDO\n")
            print(f"Resultado esperado: {resultado_esperado}")
            print(f"Resultado obtenido: {resultado_obtenido}\n")
            return False
    except Exception as e:
        print(f"Error durante la ejecución: {e}\n")
        return False



def validar_particionamiento_participantes():

    participantes = db["participantes"].find({}, {"_id": 0, "participante_id": 1})
    ids_participantes = [p["participante_id"] for p in participantes]
    return sorted(ids_participantes)

resultado_esperado_1 = [1, 2]  # IDs conocidos de prueba
ejecutar_caso_de_prueba(
    "Validar particionamiento en 'participantes'",
    validar_particionamiento_participantes,
    resultado_esperado_1,
)


def validar_particionamiento_eventos():
    
    eventos = db["eventos"].find({}, {"_id": 0, "tipo_evento": 1})
    tipos_evento = [e["tipo_evento"] for e in eventos]
    return sorted(tipos_evento)

resultado_esperado_2 = ["Baloncesto", "Fútbol"]  # Tipos de eventos de prueba
ejecutar_caso_de_prueba(
    "Validar particionamiento en 'eventos'",
    validar_particionamiento_eventos,
    resultado_esperado_2,
)


def consultar_evento():

    evento = db["eventos"].find_one({"tipo_evento": "Fútbol"})
    return evento["nombre_evento"] if evento else None

resultado_esperado_3 = "Copa Mundial 2024"
ejecutar_caso_de_prueba(
    "Consultar evento por 'tipo_evento'",
    consultar_evento,
    resultado_esperado_3,
)


def prueba_escalabilidad():

    nuevos_participantes = [
        {"participante_id": i, "nombre": f"Participante {i}", "tipo_evento": "Fútbol"}
        for i in range(1000, 1010)
    ]
    db["participantes"].insert_many(nuevos_participantes)
    return db["participantes"].count_documents({"participante_id": {"$gte": 1000}})

resultado_esperado_4 = 10
ejecutar_caso_de_prueba(
    "Escalabilidad - Inserción masiva de datos",
    prueba_escalabilidad,
    resultado_esperado_4,
)