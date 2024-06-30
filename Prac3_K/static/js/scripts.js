function createDroneMap() {
    const mapContainer = document.getElementById('drone-map');
    for (let i = 0; i < 400; i++) {
        const cell = document.createElement('div');
        cell.classList.add('drone-cell');
        mapContainer.appendChild(cell);
    }
}

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
            fetch('/registroWEB', {
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

function handleLogin() {
    const username = prompt("Introduce tu nombre de usuario:");
    const password = prompt("Introduce tu contraseña:");
    return request_jwt_token(username, password);
}

function get_jwt_token() {
    return new Promise((resolve, reject) => {
        const tiene_usuario = prompt("¿Ya tienes un usuario? (si/no)").toLowerCase();

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

function delSelectedDrones() {
    const selectedDrones = document.querySelectorAll('#drone-list input[name="drone"]:checked');
    const droneIds = Array.from(selectedDrones).map(checkbox => checkbox.value);

    fetch('/borrar_drones', { 
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ drone_ids: droneIds })
    })
    .then(response => response.json())
    .then(data => {
        console.log(data);
        updateDroneList();
    })
    .catch(error => console.error('Error al borrar los drones:', error));
}

function updateDroneList() {
    fetch('/listar_drones')
        .then(response => response.json())
        .then(drones => {
            const listElement = document.getElementById('drone-list');
            listElement.innerHTML = '';
            drones.forEach(drone => {
                const listItem = document.createElement('li');
                listItem.innerHTML = `
                    <input type="checkbox" class="drone-checkbox" name="drone" value="${drone.ID}">
                    <label for="drone${drone.ID}">ID: ${drone.ID}, Alias: ${drone.Alias}</label>
                `;
                listElement.appendChild(listItem);
            });
        });
}

setInterval(function() {
    updateDronePositions();
    updateMapWithTableData();
}, 500);

function cargarAuditoria() {
    fetch('/auditoria')
    .then(response => response.json())
    .then(data => {
        const listaDrones = document.getElementById('lista-auditoria');
        const listaEngine = document.getElementById('lista-auditoria-engine');
        const listaRegistry = document.getElementById('lista-auditoria-registry');
        listaDrones.innerHTML = ''; // Limpiar lista actual
        listaEngine.innerHTML = ''; // Limpiar lista actual
        listaRegistry.innerHTML = ''; // Limpiar lista actual

        data.general.forEach(log => {
            const item = document.createElement('li');
            item.textContent = `${log.timestamp}: ${log.evento} - ${log.descripcion}`;
            if (log.tipo === 'engine') {
                listaEngine.appendChild(item);
            } else if (log.tipo === 'registry') {
                listaRegistry.appendChild(item);
            } else {
                listaDrones.appendChild(item);
            }
        });
    })
    .catch(error => console.error('Error al cargar auditoría:', error));
}

function listenForAuditUpdates() {
    const eventSource = new EventSource('/stream_auditoria');
    eventSource.onmessage = function(event) {
        const auditoria = JSON.parse(event.data);
        const listaDrones = document.getElementById('lista-auditoria');
        const listaEngine = document.getElementById('lista-auditoria-engine');
        const listaRegistry = document.getElementById('lista-auditoria-registry');
        listaDrones.innerHTML = ''; // Limpiar lista actual
        listaEngine.innerHTML = ''; // Limpiar lista actual
        listaRegistry.innerHTML = ''; // Limpiar lista actual

        auditoria.general.forEach(log => {
            const item = document.createElement('li');
            item.textContent = `${log.timestamp}: ${log.evento} - ${log.descripcion}`;
            if (log.tipo === 'engine') {
                listaEngine.appendChild(item);
            } else if (log.tipo === 'registry') {
                listaRegistry.appendChild(item);
            } else {
                listaDrones.appendChild(item);
            }
        });
    };
}


window.onload = function() {
    createDroneMap();
    updateDroneList();
    updateDronePositions();
    updateFinalDronePositions();
    listenForAuditUpdates();
    cargarAuditoria(); // Asegúrate de llamar a esta función al cargar la página
    listenForDisconnections();
};

function listenForDisconnections() {
    const eventSource = new EventSource('/stream_errors');
    eventSource.onmessage = function(event) {
        const errors = JSON.parse(event.data);
        handleDisconnections(errors);
    };
}

function handleDisconnections(errors) {
    errors.forEach(error => {
        if (error.includes('desconectado')) {
            const droneID = error.split(' ')[1];  // Asume que el error tiene el formato "Dron <ID> desconectado"
            removeDroneFromMap(droneID);
        }
    });
}

function removeDroneFromMap(droneID) {
    const tableRows = document.querySelectorAll('#drone-positions-table tr');
    tableRows.forEach(row => {
        if (row.cells[0].textContent === droneID) {
            row.remove();
        }
    });
    updateMapWithTableData();  // Actualiza el mapa después de eliminar el dron
}

let finalDronePositions = {};

function updateDronePositions() {
    fetch('/get_drone_positions')
        .then(response => response.json())
        .then(data => {
            let tableContent = '<h3>Posiciones de Drones</h3>';
            tableContent += '<table><tr><th>ID</th><th>Posición X</th><th>Posición Y</th><th>Estado</th></tr>';
            for (let droneID in data) {
                tableContent += `<tr><td>${droneID}</td><td>${data[droneID][0]}</td><td>${data[droneID][1]}</td><td>${data[droneID][2]}</td></tr>`;
            }
            tableContent += '</table>';
            document.getElementById('drone-positions-table').innerHTML = tableContent;
        })
        .catch(error => console.error('Error al obtener posiciones de drones:', error));
}

function getDroneColor(droneID) {
    const status = finalDronePositions[droneID];
    return status === 'final' ? 'green' : 'red';
}

function updateMapWithTableData() {
    const tableRows = document.querySelectorAll('#drone-positions-table tr');
    const mapContainer = document.getElementById('drone-map');
    mapContainer.innerHTML = '';

    const dronePositions = new Map();

    for (let i = 1; i < tableRows.length; i++) {
        const cells = tableRows[i].cells;
        const droneID = cells[0].textContent; // ID del dron
        const posX = parseInt(cells[1].textContent, 10);
        const posY = parseInt(cells[2].textContent, 10);
        const droneColor = getDroneColor(droneID); // Función para obtener el color del dron

        // Agregar la posición y color del dron al mapa
        dronePositions.set(`${posX},${posY}`, { id: droneID, color: droneColor });
    }

    for (let y = 0; y < 20; y++) {
        for (let x = 0; x < 20; x++) {
            const positionKey = `${x},${y}`;
            const cell = document.createElement('div');
            cell.classList.add('drone-cell');

            if (dronePositions.has(positionKey)) {
                const droneData = dronePositions.get(positionKey);
                cell.style.backgroundColor = droneData.color; // Colorea el dron según su estado
                cell.textContent = droneData.id; // Muestra el ID del dron en la celda
            }
            mapContainer.appendChild(cell);
        }
    }
}


function listenForErrors() {
    const eventSource = new EventSource('/stream_errors');
    eventSource.onmessage = function(event) {
        const errors = JSON.parse(event.data);
        displayErrors(errors);
    };
}

function displayErrors(errors) {
    const errorList = document.getElementById('error-list');
    errorList.innerHTML = '';
    errors.forEach(error => {
        const listItem = document.createElement('li');
        listItem.textContent = error;
        errorList.appendChild(listItem);
    });
}

updateFinalDronePositions();

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
