module.exports = (connection, DataTypes) => {
  const Maintainer = connection.define('maintainers', {
    name: {
      type: DataTypes.STRING,
      unique: true
    }
    // belongsToMany: Sites
  }, {
    timestamps: false,
    underscored: true
  });
  return Maintainer;
};
