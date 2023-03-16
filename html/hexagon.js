//hexagon.js

class Hexagon {
  constructor(size) {
    this.size = size;
    this.geometry = new THREE.Geometry();

    for (let i = 0; i < 6; i++) {
      const angle = i * Math.PI / 3;
      const x = this.size * Math.cos(angle);
      const y = this.size * Math.sin(angle);
      this.geometry.vertices.push(new THREE.Vector3(x, y, 0));
    }

    for (let i = 0; i < 6; i++) {
      this.geometry.faces.push(new THREE.Face3(i, (i+1)%6, 6));
    }

    this.material = new THREE.MeshBasicMaterial({ color: 0xffffff });
    this.mesh = new THREE.Mesh(this.geometry, this.material);

    // Create hexagons inside
    const innerHexagonSize = this.size * 0.7;
    const innerHexagon = new Hexagon(innerHexagonSize);
    this.mesh.add(innerHexagon.mesh);
  }
}


