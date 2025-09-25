# Use the official .NET 9 SDK image as base
FROM mcr.microsoft.com/dotnet/sdk:9.0

# Install additional packages needed for development and USB device interaction
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        # Basic development tools
        git \
        curl \
        wget \
        unzip \
        zip \
        ca-certificates \
        # USB and hardware interaction tools
        usbutils \
        libusb-1.0-0-dev \
        pkg-config \
        # System tools for debugging
        strace \
        lsof \
        procps \
        # Additional utilities
        nano \
        vim \
        tree \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Create a non-root user to use if preferred (see devcontainer.json)
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    # Add sudo support for the non-root user
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Add the user to the dialout group for USB device access
RUN usermod -a -G dialout $USERNAME

# Set up dotnet tools path for the user
ENV PATH="$PATH:/home/$USERNAME/.dotnet/tools"

# Switch to the non-root user
USER $USERNAME

# Install useful .NET global tools
RUN dotnet tool install --global dotnet-format \
    && dotnet tool install --global dotnet-reportgenerator-globaltool

# Verify .NET installation
RUN dotnet --version

# Set the working directory
WORKDIR /workspace

# Default command
CMD ["bash"]