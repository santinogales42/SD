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
    updateDronePositions();
};

function getWeather(city) {
    fetch('/weather/' + city)
    .then(response => {
        if (!response.ok) {
            throw new Error('Respuesta del servidor no exitosa.');
        }
        return response.json();
    })
    .then(data => {

        alert("La temperatura en " + city + " es: " + data.temp + "°C");

    })
}



function register_user() {
    return new Promise((resolve, reject) => {
        const username = prompt("Ingrese su nombre de usuario para el registro:");
        const password = prompt("Ingrese su contraseña para el registro:");

        fetch('/registro_usuario', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ username: username, password: password }),
        })
        .then(response => {
            if (response.ok) {
                alert("Usuario registrado con éxito. Iniciando sesión...");
                return request_jwt_token(username, password);
            } else {
                return response.json().then(data => {
                    throw new Error("Error al registrar usuario: " + data.msg);
                });
            }
        })
        .then(token => {
            if (token) {
                resolve(token);
            } else {
                throw new Error("No se pudo obtener un token JWT válido.");
            }
        })
        .catch(error => {
            alert(error.message);
            reject(error);
        });
    });
}

function addDrone() {
    get_jwt_token()
    .then(token => {
        const droneID = prompt("Ingrese el ID del dron (debe ser un número):");
        const droneAlias = prompt("Ingrese el alias del dron:");

        if (droneID && droneAlias) {
            fetch('/registro', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + token
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
        } else {
            alert("Por favor, ingrese un ID y un alias válidos.");
        }
    })
    .catch(error => console.error('Error:', error));
}


function get_jwt_token() {
    return new Promise((resolve, reject) => {
        const tiene_usuario = prompt("¿Ya tienes un usuario? (si/no)").toLowerCase();

        function handleLogin() {
            const username = prompt("Introduce tu nombre de usuario:");
            const password = prompt("Introduce tu contraseña:");
            return request_jwt_token(username, password);
        }

        if (tiene_usuario === "no") {
            register_user()
                .then(token => resolve(token))
                .catch(error => {
                    alert("Error durante el registro: " + error.message);
                    reject(error);
                });
        } else {
            handleLogin()
                .then(token => resolve(token))
                .catch(error => {
                    alert("Error durante el inicio de sesión: " + error.message);
                    reject(error);
                });
        }
    });
}




function request_jwt_token() {
    return new Promise((resolve, reject) => {
        const username = prompt("Introduce tu nombre de usuario:");
        const password = prompt("Introduce tu contraseña:");

        fetch('/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ username: username, password: password }),
        })
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error("Error al obtener token JWT");
            }
        })
        .then(data => resolve(data.access_token))
        .catch(error => reject(error));
    });
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
            listElement.innerHTML = '';
            drones.forEach(drone => {
                const listItem = document.createElement('li');
                listItem.innerHTML = `<input type="checkbox" class="drone-checkbox" id="drone${drone.ID}" value="${drone.ID}"> <label for="drone${drone.ID}">ID: ${drone.ID}, Alias: ${drone.Alias}</label>`;
                listElement.appendChild(listItem);
            });
            /*
            const joinButton = document.createElement('button');
            joinButton.innerText = 'Unir al Show';
            joinButton.onclick = confirmJoinShow;
            listElement.appendChild(joinButton);*/
        })
        .catch(error => console.error('Error al listar drones:', error));
}

setInterval(function() {
    updateDronePositions();
    updateMapWithTableData();
}, 500); // Actualiza cada 2 segundos

function updateMapWithTableData() {
    const tableRows = document.querySelectorAll('#drone-positions-table tr');
    const occupiedPositions = new Set();

    for (let i = 1; i < tableRows.length; i++) {
        const cells = tableRows[i].cells;
        const posX = parseInt(cells[1].textContent, 10);
        const posY = parseInt(cells[2].textContent, 10);
        occupiedPositions.add(`${posX},${posY}`);
    }

    const mapContainer = document.getElementById('drone-map');
    mapContainer.innerHTML = '';

    for (let y = 0; y < 20; y++) {
        for (let x = 0; x < 20; x++) {
            const cell = document.createElement('div');
            const positionKey = `${x},${y}`;
            if (finalDronePositions[positionKey]) {
                cell.classList.add('final-cell');
            } else if (occupiedPositions.has(positionKey)) {
                cell.classList.add('occupied-cell');
            } else {
                cell.classList.add('drone-cell');
            }
            mapContainer.appendChild(cell);
        }
    }
}


function updateDronePositions() {
    fetch('/get_drone_positions')
        .then(response => response.json())
        .then(data => {
            let tableContent = '<h3>Posiciones de Drones</h3>';
            tableContent += '<table><tr><th>ID</th><th>Posición X</th><th>Posición Y</th></tr>';
            for (let droneID in data) {
                tableContent += `<tr><td>${droneID}</td><td>${data[droneID][0]}</td><td>${data[droneID][1]}</td></tr>`;
            }
            tableContent += '</table>';
            document.getElementById('drone-positions-table').innerHTML = tableContent;
        })
        .catch(error => console.error('Error al obtener posiciones de drones:', error));
}

let finalDronePositions = {};

function updateFinalDronePositions() {
    fetch('/get_final_drone_positions') // Ruta de tu API para obtener posiciones finales
        .then(response => response.json())
        .then(data => {
            finalDronePositions = data;
            console.log('Posiciones Finales de Drones:', finalDronePositions);
        })
        .catch(error => console.error('Error al obtener posiciones finales de drones:', error));
}


// Llamar a la función para actualizar las posiciones finales
updateFinalDronePositions();

window.onload = function() {
    createDroneMap();
    updateDroneList();
    updateDronePositions();
    updateFinalPositions(); // Agregar esta línea
    loadErrors();
};

function loadErrors() {
    fetch('/get_errors')
        .then(response => response.json())
        .then(errors => {
            const errorList = document.getElementById('error-list');
            errorList.innerHTML = '';
            errors.forEach(error => {
                const listItem = document.createElement('li');
                listItem.textContent = error;
                errorList.appendChild(listItem);
            });
        });
}
