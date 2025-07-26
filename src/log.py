import logging

def logs(nome_arquivo, nome_logger):    
    logger = logging.getLogger(nome_logger)  # Cria um logger específico
    logger.setLevel(logging.INFO)  # Define o nível de log

    # Remove os handlers antigos
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(nome_arquivo, mode='a', encoding='utf-8')
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Garante que o log seja gravado imediatamente no arquivo
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

    return logger  # Retorna o logger para uso posterior
