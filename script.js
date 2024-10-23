// Seleccionamos los elementos del DOM
const sky = document.querySelector('.sky');
const stars = document.querySelector('.stars');
const moon = document.querySelector('.moon');

// Ajustamos el factor de movimiento
const moveFactor = 0.05; // Ajusta cuánto se mueve el fondo con el mouse

document.addEventListener('mousemove', (event) => {
    const mouseX = event.clientX / window.innerWidth;
    const mouseY = event.clientY / window.innerHeight;

    // Mover el fondo de las estrellas basado en la posición del mouse
    stars.style.transform = `translate(${(mouseX - 0.5) * 100}px, ${(mouseY - 0.5) * 100}px)`;

    // Mover ligeramente la luna para un efecto adicional
    moon.style.transform = `translate(${(mouseX - 0.5) * 50}px, ${(mouseY - 0.5) * 50}px) scale(1.05)`;
});

// Generar estrellas aleatorias en el cielo
const starCount = 100; // Número de estrellas
for (let i = 0; i < starCount; i++) {
    let star = document.createElement('div');
    star.classList.add('star');
    star.style.top = `${Math.random() * 100}vh`; // Posición vertical aleatoria
    star.style.left = `${Math.random() * 100}vw`; // Posición horizontal aleatoria
    star.style.animationDelay = `${Math.random() * 5}s`; // Retardo aleatorio para la animación
    star.style.opacity = `${Math.random() * 0.5 + 0.5}`; // Opacidad aleatoria
    star.style.transform = `scale(${Math.random() * 0.5 + 0.5})`; // Escala aleatoria
    sky.appendChild(star); // Agrega la estrella al cielo
}

// Reproducir sonido al hacer clic en la luna
const moonSound = document.getElementById('moonSound');
moon.addEventListener('click', () => {
    moonSound.currentTime = 0; // Reinicia el sonido
    moonSound.play(); // Reproduce el sonido
});

// Manejar el esparcimiento de estrellas al mover el mouse
document.addEventListener('mousemove', (event) => {
    const x = event.clientX;
    const y = event.clientY;

    // Calcula el desplazamiento basado en la posición del mouse
    const moveX = (x / window.innerWidth - 0.5) * 50; // Modifica 50 para ajustar el movimiento
    const moveY = (y / window.innerHeight - 0.5) * 50; // Modifica 50 para ajustar el movimiento

    stars.style.transform = `translate(${moveX}px, ${moveY}px)`; // Aplica el desplazamiento a las estrellas
});

// Resetea las estrellas a su posición original cuando el mouse sale
document.addEventListener('mouseleave', () => {
    stars.style.transform = 'translate(0, 0)';
});
