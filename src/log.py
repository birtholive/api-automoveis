import logging

def logs(nome_arquivo, logger_name):    
    logger = logging.getLogger(logger_name)  # Cria um logger específico
    logger.setLevel(logging.INFO)  # Define o nível de log

    # # Verifica se o logger já tem handlers para evitar duplicação
    # if not logger.hasHandlers():
    file_handler = logging.FileHandler(nome_arquivo, mode='a', encoding='utf-8')
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Garante que o log seja gravado imediatamente no arquivo
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

    return logger  # Retorna o logger para uso posterior
