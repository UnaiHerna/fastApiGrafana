from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Create the async engine and sessionmaker
DATABASE_URL = "mysql+aiomysql://root:Cim12345!@localhost/datos"
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session
