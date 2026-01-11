from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, JsonConfigSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict


class Proxy(BaseModel):
    enabled: bool = Field(default=True, description="Enable proxy usage")
    host: str = Field(description="Proxy host")
    port: int = Field(description="Proxy port")
    username: str = Field(default="", description="Proxy username")
    password: str = Field(default="", description="Proxy password")


class Config(BaseSettings):
    model_config = SettingsConfigDict(json_file="config.json", extra="allow")

    headless_mode: bool = Field(default=True, description="Run the scraper in headless mode")
    delay: float = Field(default=0.2, description="Delay between requests in milliseconds")
    page_timeout_ms: int = Field(default=5000, description="Timeout for page loading in milliseconds")
    locator_timeout: int = Field(default=2000, description="Timeout for locator in milliseconds")
    retries: int = Field(default=3, description="Number of retries for failed requests")
    max_concurrent: int = Field(default=5, description="Maximum number of concurrent requests")
    batch_size: int = Field(default=10, description="Batch size for processing")

    user_agents: list[str] = Field(
        default=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        ],
        description="List of user agents to use for requests",
    )

    input_file_path: str = Field(default="", description="Path to the input file")
    output_file_path: str = Field(default="", description="Path to the output file")

    proxy: Proxy | None = Field(default=None, description="Proxy configuration")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (JsonConfigSettingsSource(settings_cls),)
