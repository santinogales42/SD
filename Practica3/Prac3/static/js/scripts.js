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

function addDrone() {
    const droneName = prompt("Ingrese el nombre del dron (debe ser un número):");

    if (droneName !== null && !isNaN(droneName)) {
        const droneList = document.getElementById('drone-list');
        const newDrone = document.createElement('li');
        newDrone.textContent = 'Dron ' + droneName;
        droneList.appendChild(newDrone);
    } else {
        alert("Por favor, ingrese un número válido como nombre del dron.");
    }
}

window.onload = createDroneMap;

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