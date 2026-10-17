require 'pangea-core'

template :lava_ruby_qualification do
  resource :random_pet, :qualification do
    length 2
    prefix ENV.fetch('LAVA_QUAL_PREFIX', 'lava')
  end

  output :pet_id do
    value '${random_pet.qualification.id}'
  end
end
