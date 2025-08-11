document.addEventListener("DOMContentLoaded", () => {
  const socket = io();
  let lastFilmsData = [];
  let filteredFilmsData = [];
  let lastUsersData = [];
  let currentPage = 1;
  const filmsPerPage = 5;

  function init() {
    showSection("addFilmSection");
    updateWelcomeMessage();
    initializeGSAPAnimations();
    setupEventListeners();
    setupFilmForm();
    setupSearchAndFilter();
    startFilmListUpdates();
    startUserListUpdates();
  }

  function setupEventListeners() {
    document.querySelectorAll(".nav-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        const button = e.currentTarget;
        const sectionId = button.id.replace("Btn", "Section");
        showSection(sectionId);
        updateActiveButton(button);
      });
    });

    const closeBtn = document.querySelector(".close");
    if (closeBtn) {
      closeBtn.addEventListener("click", closeModal);
    }

    window.addEventListener("click", (e) => {
      if (e.target === document.getElementById("editFilmModal")) {
        closeModal();
      }
    });
  }

  function showSection(sectionId) {
    document.querySelectorAll(".section").forEach((section) => {
      if (section.id !== sectionId) {
        section.classList.remove("active");
        gsap.to(section, {
          opacity: 0,
          y: 20,
          duration: 0.1,
          onComplete: () => {
            section.style.display = "none";
          },
        });
      }
    });

    const activeSection = document.getElementById(sectionId);
    if (activeSection) {
      activeSection.style.display = "block";
      gsap.fromTo(
        activeSection,
        { opacity: 0, y: 20 },
        {
          opacity: 1,
          y: 0,
          duration: 0.3,
          delay: 0.1,
          onComplete: () => {
            activeSection.classList.add("active");
          },
        }
      );
    }
  }

  function updateActiveButton(button) {
    document.querySelectorAll(".nav-btn").forEach((btn) => {
      btn.classList.remove("active");
    });
    button.classList.add("active");
  }

  function updateWelcomeMessage() {
    const welcomeMessage = document.getElementById("welcome-message");
    const currentHour = new Date().getHours();
    let greeting;
    if (currentHour < 12) greeting = "Ебать";
    else if (currentHour < 18) greeting = "Ахуеть";
    else greeting = "Нихуя себе";
    welcomeMessage.textContent = `${greeting}, Уга Буга зашел!`;
  }

  function initializeGSAPAnimations() {
    gsap.from("header", {
      duration: 1,
      y: -50,
      opacity: 0,
      ease: "power3.out",
    });

    gsap.from(".nav-btn", {
      duration: 0.5,
      opacity: 0,
      y: 20,
      stagger: 0.1,
      ease: "power2.out",
    });
  }

  function updateFilmList(films) {
    const tbody = document.querySelector("#filmList tbody");
    if (!tbody) return;

    tbody.innerHTML = "";
    const startIndex = (currentPage - 1) * filmsPerPage;
    const endIndex = startIndex + filmsPerPage;
    const paginatedFilms = films.slice(startIndex, endIndex);

    paginatedFilms.forEach((film) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${film.id}</td>
        <td>${film.name}</td>
        <td>${film.genre}</td>
        <td>${film.site ? `<a href="${film.site}" target="_blank" class="text-blue-400 hover:text-blue-300">Перейти</a>` : "Не указан"}</td>
        <td>${
          film.photo_id
            ? `<img src="/static/uploads/${film.photo_id}" alt="${film.name}" class="film-thumbnail">`
            : "Нет изображения"
        }</td>
        <td>
          <div class="flex gap-2">
            <button onclick="handleEditFilm(${film.id})" class="btn-edit">
              <i class="fas fa-edit"></i>
            </button>
            <button onclick="handleDeleteFilm(${film.id})" class="btn-delete">
              <i class="fas fa-trash"></i>
            </button>
          </div>
        </td>
      `;
      tbody.appendChild(row);
    });

    updatePagination(films.length);
  }

  function updatePagination(totalFilms) {
    const totalPages = Math.ceil(totalFilms / filmsPerPage);
    const paginationContainer = document.getElementById("pagination");
    paginationContainer.innerHTML = "";

    for (let i = 1; i <= totalPages; i++) {
      const button = document.createElement("button");
      button.textContent = i;
      button.classList.add("pagination-btn");
      if (i === currentPage) {
        button.classList.add("active");
      }
      button.addEventListener("click", () => {
        currentPage = i;
        updateFilmList(filteredFilmsData.length > 0 ? filteredFilmsData : lastFilmsData);
      });
      paginationContainer.appendChild(button);
    }
  }

  function updateChart(films) {
    const ctx = document.getElementById("filmChart");
    if (!ctx) return;

    const genreCounts = {};
    films.forEach((film) => {
      genreCounts[film.genre] = (genreCounts[film.genre] || 0) + 1;
    });

    if (window.filmChart) {
      window.filmChart.destroy();
    }

    window.filmChart = new Chart(ctx, {
      type: "doughnut",
      data: {
        labels: Object.keys(genreCounts),
        datasets: [
          {
            data: Object.values(genreCounts),
            backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"],
            borderColor: "#132f4c",
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "right",
            labels: {
              color: "#ffffff",
            },
          },
          title: {
            display: true,
            text: "Распределение фильмов по жанрам",
            color: "#ffffff",
            font: {
              size: 16,
            },
          },
        },
      },
    });
  }

  function updateRecentAdditions(films) {
    const recentList = document.getElementById("recentAdditions");
    if (!recentList) return;

    recentList.innerHTML = "";
    films.slice(0, 5).forEach((film) => {
      const li = document.createElement("li");
      li.textContent = `${film.name} (${film.genre})`;
      recentList.appendChild(li);
    });
  }

  function updateUserList(users) {
    const tbody = document.querySelector("#userList tbody");
    if (!tbody) return;

    tbody.innerHTML = "";
    users.forEach((user) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${user.id}</td>
        <td>${user.name}</td>
        <td>${user.tg_id}</td>
        <td>${user.admin ? '<span class="text-green-400">Траффер</span>' : "Пользователь"}</td>
        <td>
          <div class="flex gap-2">
            <button onclick="handleToggleAdmin(${user.id}, ${user.admin})" class="btn-admin">
              <i class="fas fa-${user.admin ? 'user-minus' : 'user-plus'}"></i>
            </button>
            <button onclick="handleBanUser(${user.id})" class="btn-ban" ${user.banned ? 'disabled' : ''}>
              <i class="fas fa-ban"></i>
            </button>
          </div>
        </td>
      `;
      tbody.appendChild(row);
    });
  }

  function showNotification(message, type) {
    const notification = document.createElement("div");
    notification.className = `notification ${type}`;
    notification.textContent = message;

    const container = document.getElementById("notificationContainer");
    container.appendChild(notification);

    // Ограничиваем до 4 уведомлений: плавно удаляем самое старое при превышении
    const MAX = 4;
    while (container.children.length > MAX) {
      const oldest = container.firstElementChild;
      if (!oldest) break;
      if (oldest !== notification) {
        gsap.to(oldest, {
          x: -100,
          opacity: 0,
          duration: 0.5,
          ease: "power2.in",
          onComplete: () => { oldest.remove(); },
        });
      } else {
        break;
      }
    }

    gsap.fromTo(
      notification,
      { x: -100, opacity: 0 },
      {
        x: 0,
        opacity: 1,
        duration: 0.5,
        ease: "power2.out",
        onComplete: () => {
          setTimeout(() => {
            gsap.to(notification, {
              x: -100,
              opacity: 0,
              duration: 0.5,
              ease: "power2.in",
              onComplete: () => { notification.remove(); },
            });
          }, 3000);
        },
      }
    );
  }

  async function handleAddFilmSubmit(e) {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);

    try {
      const response = await fetch("/api/film", {
        method: "POST",
        body: formData,
      });
      const data = await response.json();

      if (response.ok) {
        showNotification(`Фильм "${data.name}" успешно добавлен`, "success");
        form.reset();
        document.getElementById("imagePreview").innerHTML = "";
        socket.emit("get_films");
      } else {
        throw new Error(data.message || "Ошибка при добавлении фильма");
      }
    } catch (error) {
      showNotification(error.message, "error");
    }
  }

  async function handleEditFilmSubmit(e) {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    const filmId = document.getElementById("editFilmId").value;

    try {
      const response = await fetch(`/api/film/${filmId}`, {
        method: "PUT",
        body: formData,
      });
      const data = await response.json();

      if (response.ok) {
        showNotification("Фильм успешно обновлен", "success");
        closeModal();
        socket.emit("get_films");
      } else {
        throw new Error(data.message || "Ошибка при обновлении фильма");
      }
    } catch (error) {
      showNotification(error.message, "error");
    }
  }

  window.handleEditFilm = async (id) => {
    try {
      const response = await fetch(`/api/film/${id}`);
      const film = await response.json();

      if (response.ok) {
        document.getElementById("editFilmId").value = film.id;
        document.getElementById("editFilmName").value = film.name;
        document.getElementById("editFilmGenre").value = film.genre;
        document.getElementById("editFilmDescription").value = film.description;
        document.getElementById("editFilmSite").value = film.site || "";

        const editImagePreview = document.getElementById("editImagePreview");
        if (film.photo_id) {
          editImagePreview.innerHTML = `
            <div class="image-preview-container">
              <img src="/static/uploads/${film.photo_id}" alt="${film.name}" class="preview-image">
              <button type="button" class="remove-image">&times;</button>
            </div>
          `;
        } else {
          editImagePreview.innerHTML = "";
        }

        const modal = document.getElementById("editFilmModal");
        modal.style.display = "block";
        gsap.fromTo(modal, { opacity: 0 }, { opacity: 1, duration: 0.3 });
      } else {
        throw new Error("Ошибка при загрузке данных фильма");
      }
    } catch (error) {
      showNotification(error.message, "error");
    }
  };

  window.handleDeleteFilm = (id) => {
    if (confirm("Вы уверены, что хотите удалить этот фильм?")) {
      socket.emit("delete_film", id);
    }
  };

  window.handleToggleAdmin = async (id, isAdmin) => {
    try {
      const response = await fetch(`/api/user/${id}/toggle-admin`, {
        method: "POST",
      });
      const data = await response.json();

      if (response.ok) {
        showNotification(data.message, "success");
        socket.emit("get_users");
      } else {
        throw new Error(data.message || "Ошибка при изменении статуса пользователя");
      }
    } catch (error) {
      showNotification(error.message, "error");
    }
  };

  window.handleBanUser = async (id) => {
    if (confirm("Вы уверены, что хотите забанить этого пользователя?")) {
      try {
        const response = await fetch(`/api/user/${id}/ban`, {
          method: "POST",
        });
        const data = await response.json();

        if (response.ok) {
          showNotification(data.message, "success");
          socket.emit("get_users");
        } else {
          throw new Error(data.message || "Ошибка при бане пользователя");
        }
      } catch (error) {
        showNotification(error.message, "error");
      }
    }
  };

  function handleFilmSearch(event) {
    currentPage = 1;
    const searchTerm = event.target.value.toLowerCase();
    filteredFilmsData = lastFilmsData.filter(
      (film) =>
        film.name.toLowerCase().includes(searchTerm) ||
        film.id.toString() === searchTerm
    );
    updateFilmList(filteredFilmsData);
    updatePagination(filteredFilmsData.length);
  }

  function handleGenreFilter(event) {
    currentPage = 1;
    const selectedGenre = event.target.value;
    filteredFilmsData =
      selectedGenre === "all"
        ? lastFilmsData
        : lastFilmsData.filter((film) => film.genre === selectedGenre);
    updateFilmList(filteredFilmsData);
    updatePagination(filteredFilmsData.length);
  }

  function closeModal() {
    const modal = document.getElementById("editFilmModal");
    gsap.to(modal, {
      opacity: 0,
      duration: 0.3,
      onComplete: () => {
        modal.style.display = "none";
      },
    });
  }

  function setupFilmForm() {
    const addFilmForm = document.getElementById("addFilmForm");
    if (addFilmForm) {
      addFilmForm.addEventListener("submit", handleAddFilmSubmit);
    }

    const editFilmForm = document.getElementById("editFilmForm");
    if (editFilmForm) {
      editFilmForm.addEventListener("submit", handleEditFilmSubmit);
    }

    document.getElementById("filmImage").addEventListener("change", handleImagePreview);
    document.getElementById("editFilmImage").addEventListener("change", handleImagePreview);
  }

  function handleImagePreview(e) {
    const file = e.target.files[0];
    const previewId = e.target.id === "filmImage" ? "imagePreview" : "editImagePreview";
    const preview = document.getElementById(previewId);
    preview.innerHTML = "";

    if (file) {
      const reader = new FileReader();
      reader.onload = function (e) {
        preview.innerHTML = `
          <div class="image-preview-container">
            <img src="${e.target.result}" alt="Preview" class="preview-image">
            <button type="button" class="remove-image">&times;</button>
          </div>
        `;
      };
      reader.readAsDataURL(file);
    }
  }

  function setupSearchAndFilter() {
    const searchInput = document.getElementById("searchFilm");
    if (searchInput) {
      searchInput.addEventListener("input", handleFilmSearch);
    }

    const genreFilter = document.getElementById("filterGenre");
    if (genreFilter) {
      genreFilter.addEventListener("change", handleGenreFilter);
    }
  }

  function startFilmListUpdates() {
    setInterval(() => {
      socket.emit("get_films");
    }, 200);
  }

  function startUserListUpdates() {
    setInterval(() => {
      socket.emit("get_users");
    }, 200);
  }

  socket.on("update_films", (films) => {
    lastFilmsData = films;
    filteredFilmsData = films;
    updateFilmList(films);
    updateChart(films);
    updateRecentAdditions(films);
    updatePagination(films.length);
  });

  socket.on("update_users", (users) => {
    lastUsersData = users;
    updateUserList(users);
  });

  socket.on("notification", (data) => {
    showNotification(data.message, data.type);
  });

  init();
});