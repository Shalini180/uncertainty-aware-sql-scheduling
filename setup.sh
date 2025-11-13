#!/bin/bash

# Energy ML Project Setup Script
# This script sets up the complete environment

set -e

echo "🌱 Carbon-Aware Query Engine Setup"
echo "===================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        echo "Please install Docker from https://docs.docker.com/get-docker/"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker found${NC}"
}

# Check if Docker Compose is installed
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}❌ Docker Compose is not installed${NC}"
        echo "Please install Docker Compose from https://docs.docker.com/compose/install/"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker Compose found${NC}"
}

# Create necessary directories
create_directories() {
    echo ""
    echo "📁 Creating directories..."
    mkdir -p data logs backups alembic/versions
    echo -e "${GREEN}✓ Directories created${NC}"
}

# Setup environment file
setup_env() {
    echo ""
    echo "⚙️  Setting up environment..."
    
    if [ ! -f .env ]; then
        cp .env.example .env
        echo -e "${GREEN}✓ .env file created${NC}"
        echo -e "${YELLOW}⚠️  Please edit .env with your settings${NC}"
        
        # Generate a random secret key
        SECRET_KEY=$(openssl rand -hex 32)
        
        # Update .env with generated secret
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s/your-secret-key-here/$SECRET_KEY/" .env
        else
            sed -i "s/your-secret-key-here/$SECRET_KEY/" .env
        fi
        
        echo -e "${GREEN}✓ Generated SECRET_KEY${NC}"
    else
        echo -e "${YELLOW}⚠️  .env already exists, skipping${NC}"
    fi
}

# Create Alembic directory structure if it doesn't exist
setup_alembic() {
    echo ""
    echo "🗄️  Setting up database migrations..."
    
    if [ ! -d "alembic" ]; then
        mkdir -p alembic/versions
        cat > alembic/env.py << 'EOF'
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.db.models import Base

config = context.config
fileConfig(config.config_file_name)

# Override sqlalchemy.url with environment variable
database_url = os.getenv("DATABASE_URL")
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)

target_metadata = Base.metadata

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
EOF
        
        cat > alembic/script.py.mako << 'EOF'
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
EOF
        echo -e "${GREEN}✓ Alembic initialized${NC}"
    else
        echo -e "${YELLOW}⚠️  Alembic already exists, skipping${NC}"
    fi
}

# Build Docker images
build_images() {
    echo ""
    echo "🐳 Building Docker images..."
    docker-compose build
    echo -e "${GREEN}✓ Docker images built${NC}"
}

# Start services
start_services() {
    echo ""
    echo "🚀 Starting services..."
    docker-compose up -d
    
    echo ""
    echo "⏳ Waiting for services to be ready..."
    sleep 10
    
    # Check if services are running
    if docker-compose ps | grep -q "Up"; then
        echo -e "${GREEN}✓ Services started successfully${NC}"
    else
        echo -e "${RED}❌ Some services failed to start${NC}"
        docker-compose ps
        exit 1
    fi
}

# Initialize database
init_database() {
    echo ""
    echo "🗄️  Initializing database..."
    
    # Wait a bit more for PostgreSQL to be fully ready
    sleep 5
    
    docker-compose exec -T app python << 'EOF'
from src.db.database import init_db
try:
    init_db()
    print("✓ Database initialized")
except Exception as e:
    print(f"❌ Failed to initialize database: {e}")
    exit(1)
EOF
    
    echo -e "${GREEN}✓ Database initialized${NC}"
}

# Run health checks
health_check() {
    echo ""
    echo "🏥 Running health checks..."
    
    # Check API
    if curl -f http://localhost:8000/health &> /dev/null; then
        echo -e "${GREEN}✓ API is healthy${NC}"
    else
        echo -e "${RED}❌ API health check failed${NC}"
    fi
    
    # Check database
    if docker-compose exec -T postgres pg_isready -U admin &> /dev/null; then
        echo -e "${GREEN}✓ Database is healthy${NC}"
    else
        echo -e "${RED}❌ Database health check failed${NC}"
    fi
    
    # Check Redis
    if docker-compose exec -T redis redis-cli ping &> /dev/null; then
        echo -e "${GREEN}✓ Redis is healthy${NC}"
    else
        echo -e "${RED}❌ Redis health check failed${NC}"
    fi
}

# Print success message
print_success() {
    echo ""
    echo "======================================"
    echo -e "${GREEN}🎉 Setup completed successfully!${NC}"
    echo "======================================"
    echo ""
    echo "📱 Access the application:"
    echo "   • Streamlit UI:  http://localhost:8501"
    echo "   • API:           http://localhost:8000"
    echo "   • API Docs:      http://localhost:8000/docs"
    echo ""
    echo "🔧 Useful commands:"
    echo "   • View logs:     docker-compose logs -f"
    echo "   • Stop services: docker-compose down"
    echo "   • Restart:       docker-compose restart"
    echo "   • See all:       make help"
    echo ""
    echo -e "${YELLOW}📝 Don't forget to edit .env if needed!${NC}"
    echo ""
}

# Main setup flow
main() {
    echo "Checking prerequisites..."
    check_docker
    check_docker_compose
    
    create_directories
    setup_env
    setup_alembic
    build_images
    start_services
    init_database
    health_check
    print_success
}

# Run main function
main