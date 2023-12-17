function toggleDropdown() {
    var dropdownMenu = document.getElementById("dropdown-menu");
    dropdownMenu.style.display = (dropdownMenu.style.display === "block") ? "none" : "block";
}

function createDroneMap() {
    const mapContainer = document.getElementById('drone-map');
    for (let i = 0; i < 400; i++) {
        const cell = document.createElement('div');
        cell.classList.add('drone-cell');
        mapContainer.appendChild(cell);
    }
}


window.onload = function() {
    createDroneMap();
    updateDroneList();
};

function getWeather(city) {
    fetch('/weather/' + city)
    .then(response => response.json())
    .then(data => {
        if(data.temperature) {
            alert("La temperatura en " + city + " es: " + data.temperature + "°C");
        } else {
            alert("Error al obtener el clima.");
        }
    });
}

function addDrone() {
    const droneID = prompt("Ingrese el ID del dron (debe ser un número):");
    const droneAlias = prompt("Ingrese el alias del dron:");

    if (droneID && droneAlias) {
        fetch('/registro', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ ID: droneID, Alias: droneAlias }),
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                alert("Dron registrado correctamente con ID: " + data.drone_id);
                updateDroneList();
            } else {
                alert("Error al registrar el dron: " + data.error);
            }
        })
        .catch((error) => {
            console.error('Error:', error);
            alert("Error en la conexión con el servidor.");
        });
    } else {
        alert("Por favor, ingrese un ID y un alias válidos.");
    }
}

function delDrone() {
    const droneID = prompt("Ingrese el ID del dron a eliminar:");
    if (droneID) {
        deleteDrone(droneID);
    } else {
        alert("Por favor, ingrese un ID válido.");
    }
}

function deleteDrone(droneId) {
    fetch('/borrar_dron/' + droneId, {
        method: 'DELETE',
    })
    .then(response => {
        if (response.ok) {
            alert("Dron eliminado correctamente.");
            updateDroneList();
        } else {
            alert("Error al eliminar el dron.");
        }
    })
    .catch(error => {
        console.error('Error:', error);
        alert("Error en la conexión con el servidor.");
    });
}

function updateDroneList() {
    fetch('/listar_drones')
        .then(response => response.json())
        .then(drones => {
            const listElement = document.getElementById('drone-list');
            listElement.innerHTML = ''; // Limpia la lista actual

            drones.forEach(drone => {
                const listItem = document.createElement('li');
                listItem.textContent = `ID: ${drone.id}, Alias: ${drone.alias}`;
                listElement.appendChild(listItem);
            });
        })
        .catch(error => console.error('Error al listar drones:', error));
}
