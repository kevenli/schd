from sqlmodel import SQLModel, create_engine

engine = None

def init_db(database_url):
    global engine
    if engine is not None:
        # return engine
        raise RuntimeError("engine already initiated.")
    
    engine = create_engine(database_url, echo=True)
    # SQLModel.metadata.create_all(engine)
    return engine
