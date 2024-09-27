from src.service.reporte_plaft_service import reporte_plaft_service

def main():
    resultado = reporte_plaft_service()
    print("Resultado del proceso:", resultado)

if __name__ == "__main__":
    main()
